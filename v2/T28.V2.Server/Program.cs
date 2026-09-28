using System.Net.WebSockets;
using T28.V2.Contracts;
using T28.V2.Server;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.ConfigureHttpJsonOptions(options => options.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));
WebApplication app = builder.Build(); app.UseWebSockets();
ILogger logger = app.Logger;
bool testControls = app.Environment.IsEnvironment("Testing") && !string.IsNullOrWhiteSpace(builder.Configuration["T28_V2_TEST_CONTROL_TOKEN"]);
string? testControlToken = builder.Configuration["T28_V2_TEST_CONTROL_TOKEN"];
object acknowledgementGateLock = new(); TaskCompletionSource<bool>? acknowledgementGate = null;
TimeSpan completionTimeout = testControls && int.TryParse(builder.Configuration["T28_V2_TEST_COMPLETION_TIMEOUT_MS"], out int configuredCompletionTimeout) && configuredCompletionTimeout is >= 250 and <= 10_000 ? TimeSpan.FromMilliseconds(configuredCompletionTimeout) : TimeSpan.FromSeconds(10);
string origin = builder.Configuration["T28_V2_ORIGIN"] ?? "https://localhost";
string host = builder.Configuration["T28_V2_HOST"] ?? "localhost";
string runtime = builder.Configuration["T28_V2_RUNTIME"] ?? Path.Combine(Path.GetTempPath(), "t28-v2-runtime");
FleetAgentConfiguration? fleetOwner = FleetAgentConfiguration.Parse(
    builder.Configuration["T28_V2_FLEET_URL"],
    builder.Configuration["T28_V2_FLEET_RECEIVER_ID"],
    builder.Configuration["T28_V2_FLEET_AGENT_TOKEN"]);
string defaultReceiverId = fleetOwner?.ReceiverId ?? "lab-demo-hill";
FleetRole browserRole = string.Equals(builder.Configuration["T28_V2_ROLE"], "listener", StringComparison.OrdinalIgnoreCase) ? FleetRole.Listener : FleetRole.Admin;
V2SessionIssuer issuer = new(origin, host, browserRole); V2BrowserSecurity security = new(origin, host, "t28_v2_session", issuer.Session);
FleetRegistry fleet = new(new JsonFleetStore(Path.Combine(runtime, "fleet.json")));
string defaultReceiverDisplayName = fleet.Snapshot(defaultReceiverId).DisplayName;
FleetAudioHub fleetAudio = new();
string? agentToken = fleetOwner?.AgentToken;
string? lifecyclePepper = builder.Configuration["T28_V2_LIFECYCLE_PEPPER"];
ReceiverLifecycleRegistry? lifecycle = string.IsNullOrWhiteSpace(lifecyclePepper) ? null : new(new JsonReceiverLifecycleStore(Path.Combine(runtime, "receiver-lifecycle.json")), Convert.FromHexString(lifecyclePepper));
List<BrowserListener> listeners = []; object listenersLock = new();
Dictionary<string, (FleetSession Session, string CredentialId)> pendingProofSessions = [];
LocalReceiverSession? local = null;
if (fleetOwner is null)
{
    local = new(
        Path.Combine(runtime, "receiver.v2.sock"),
        (generation, index, pcm) => { lock (listenersLock) foreach (BrowserListener listener in listeners) listener.Publish(generation, index, pcm); return Task.CompletedTask; },
        state => { lock (listenersLock) foreach (BrowserListener listener in listeners) { if (state is null) listener.RetireGeneration(); else listener.BindGeneration(state.Generation); } });
    local.Start(); LocalReceiverSession legacy = local;
    _ = Task.Run(async () => { while (!app.Lifetime.ApplicationStopping.IsCancellationRequested) { try { await legacy.RunOneAsync(app.Lifetime.ApplicationStopping); } catch (OperationCanceledException) { } catch (EndOfStreamException) { } catch (IOException) { } catch (ObjectDisposedException) { } } });
    app.Lifetime.ApplicationStopping.Register(() => legacy.DisposeAsync().AsTask().GetAwaiter().GetResult());
}
_ = Task.Run(async () =>
{
    using PeriodicTimer sweep = new(TimeSpan.FromSeconds(1));
    try { while (await sweep.WaitForNextTickAsync(app.Lifetime.ApplicationStopping)) foreach (FleetSnapshot receiver in fleet.List()) if (receiver.Connectivity != FleetConnectivity.Online || !receiver.ProjectionCurrent) fleetAudio.Retire(receiver.ReceiverId); }
    catch (OperationCanceledException) when (app.Lifetime.ApplicationStopping.IsCancellationRequested) { }
});
bool Admin(HttpContext context) => issuer.Role == FleetRole.Admin && security.MatchesOriginHost(context.Request.Headers.Origin, context.Request.Host.Value) && security.HasSession(context.Request.Headers.Cookie) && issuer.CheckCsrf(context.Request.Headers["X-T28-V2-CSRF"]);
AgentAuthentication? Agent(HttpContext context, string receiverId, bool allowPendingProof = false, bool allowConfiguredDefaultActivation = false)
{
    if (context.Request.Headers.Authorization.Count != 1) return null;
    string authorization = context.Request.Headers.Authorization[0] ?? "";
    string? token = authorization.StartsWith("T28-Agent ", StringComparison.Ordinal) && authorization.Length > 10
        ? authorization[10..]
        : lifecycle is null && receiverId == defaultReceiverId && authorization.StartsWith("Bearer ", StringComparison.Ordinal) && authorization.Length > 7
            ? authorization[7..]
            : null;
    if (token is null) return null;
    if (lifecycle is not null)
    {
        try { AgentAuthentication authenticated = lifecycle.Authenticate(token, allowPendingProof); if (authenticated.ReceiverId == receiverId) return authenticated; }
        catch (UnauthorizedAccessException) { }
        return lifecycle.AuthenticateConfiguredDefault(receiverId, defaultReceiverDisplayName, token, fleetOwner?.ReceiverId, agentToken, allowConfiguredDefaultActivation);
    }
    return agentToken is not null && FixedTimeSecretEquals(token, agentToken) ? new(receiverId, "legacy", AgentCredentialStatus.Active, ReceiverLifecycleState.Active, 1) : null;
}
bool ListenerVisible(string receiverId)
{
    if (lifecycle is null) return true;
    try { return lifecycle.Get(receiverId).LifecycleState == ReceiverLifecycleState.Active; }
    catch (KeyNotFoundException) { return false; }
}
FleetSnapshot[] BrowserFleet(bool listenerOnly = false) => fleet.List()
    .Where(receiver => !listenerOnly || ListenerVisible(receiver.ReceiverId))
    .ToArray();
app.Use(async (context, next) =>
{
    string path = context.Request.Path.Value ?? "";
    if (path is "/v2/" or "/listener/" or "/admin/") context.Request.Path = path.TrimEnd('/');
    await next();
});
app.MapGet("/favicon.ico", () => Results.NoContent());
bool testInstrumentation = builder.Environment.IsEnvironment("Testing") && Environment.GetEnvironmentVariable("T28_V2_TEST_INSTRUMENTATION") == "1";
string instrumentation = testInstrumentation ? "{primes:0,reprimes:0,underruns:0,maxQueue:0,played:[],received:[],closeCategory:null,squelchRequests:0,squelchAcknowledgements:0,channelRequests:0,channelAcknowledgements:0}" : "null";
app.MapGet("/", () => Results.Redirect("/listener"));
app.MapGet("/v2", () => Results.Redirect("/listener"));
app.MapGet("/listener", () => Results.Content(Portal.ListenerIndexHtml, "text/html"));
app.MapGet("/listener/receiver", () => Results.Content(Portal.Page("listener", fleet: false, instrumentation: instrumentation), "text/html"));
app.MapGet("/admin", () => Results.Content(Portal.Page("admin", fleet: true, instrumentation: instrumentation), "text/html"));
app.MapGet("/v2/worklet.js", () => Results.Content(Portal.Worklet, "text/javascript"));
app.MapGet("/v2/libopus-wasm.js", () => Results.File(Path.Combine(AppContext.BaseDirectory, "libopus-wasm.js"), "text/javascript; charset=utf-8"));
app.MapGet("/v2/generated/libopus.generated.mjs", () => Results.File(Path.Combine(AppContext.BaseDirectory, "generated", "libopus.generated.mjs"), "text/javascript; charset=utf-8"));
app.MapPost("/v2/session", (HttpContext context) =>
{
    if (!security.MatchesOriginHost(context.Request.Headers.Origin, context.Request.Host.Value)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    bool listenerOnly = string.Equals(context.Request.Query["view"], "listener", StringComparison.Ordinal);
    context.Response.Cookies.Append("t28_v2_session", issuer.Session, new CookieOptions { Secure = true, HttpOnly = true, SameSite = SameSiteMode.Strict, Path = "/v2" });
    if (fleetOwner is not null)
    {
        FleetSnapshot receiver = fleet.Snapshot(defaultReceiverId); FleetSafeProjection projection = receiver.Projection;
        bool current = DefaultFleetCurrent(receiver) && projection.Channels is { Count: > 0 };
        return Results.Json(new { csrf = issuer.Csrf, role = issuer.Role.ToString().ToLowerInvariant(), managementAvailable = lifecycle is not null, fleet = BrowserFleet(listenerOnly), selectedReceiverId = defaultReceiverId, ownershipMode = "fleet-agent", receiver, state = current ? FleetCompatibilityState(projection) : null, channels = current ? projection.Channels! : Array.Empty<ApprovedChannelProjection>(), channel = new { status = current ? "applied" : "unavailable", reason = current ? null : FleetUnavailableReason(receiver) } });
    }
    ReceiverSessionSnapshot snapshot = local!.Snapshot;
    return Results.Json(new { csrf = issuer.Csrf, role = issuer.Role.ToString().ToLowerInvariant(), managementAvailable = lifecycle is not null, fleet = BrowserFleet(listenerOnly), selectedReceiverId = defaultReceiverId, ownershipMode = "legacy-local", receiver = (object?)null, state = snapshot.State, channels = snapshot.Channels, channel = snapshot.Channel });
});
app.MapGet("/v2/events", async (HttpContext context) =>
{
    if (!security.HasSession(context.Request.Headers.Cookie)) { context.Response.StatusCode = StatusCodes.Status401Unauthorized; return; }
    bool listenerOnly = string.Equals(context.Request.Query["view"], "listener", StringComparison.Ordinal);
    context.Response.ContentType = "text/event-stream; charset=utf-8";
    context.Response.Headers.CacheControl = "no-store";
    context.Response.Headers.Append("X-Accel-Buffering", "no");
    await context.Response.WriteAsync("retry: 2000\n\n", context.RequestAborted);
    long eventId = 0;
    try
    {
        while (!context.RequestAborted.IsCancellationRequested)
        {
            string payload = System.Text.Json.JsonSerializer.Serialize(new { fleet = BrowserFleet(listenerOnly), serverTime = DateTimeOffset.UtcNow });
            await context.Response.WriteAsync($"id: {++eventId}\nevent: fleet\ndata: {payload}\n\n", context.RequestAborted);
            await context.Response.Body.FlushAsync(context.RequestAborted);
            await Task.Delay(TimeSpan.FromSeconds(1), context.RequestAborted);
        }
    }
    catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { }
});
app.MapGet("/v2/diagnostics", (HttpContext context) => !security.HasSession(context.Request.Headers.Cookie)
    ? Results.Unauthorized()
    : Results.Json(new { receiverOwnershipMode = fleetOwner is null ? "legacy-local" : "fleet-agent", defaultReceiverId, localSocketEnabled = local is not null }));
app.MapGet("/v2/receivers", (HttpContext context) =>
    !security.HasSession(context.Request.Headers.Cookie) ? Results.Unauthorized() : Results.Json(fleet.List()));
app.MapGet("/v2/receivers/{receiverId}", (string receiverId, HttpContext context) =>
{
    if (!security.HasSession(context.Request.Headers.Cookie)) return Results.Unauthorized();
    try { return Results.Json(fleet.Snapshot(receiverId)); } catch (KeyNotFoundException) { return Results.NotFound(); }
});
app.MapGet("/v2/receivers/{receiverId}/controls", (string receiverId, HttpContext context) =>
{
    if (!security.HasSession(context.Request.Headers.Cookie)) return Results.Unauthorized();
    try { FleetSnapshot snapshot = fleet.Snapshot(receiverId); FleetSafeProjection p = snapshot.Projection; if (!DefaultFleetCurrent(snapshot) || p.Channels is not { Count: > 0 }) return Results.Json(new { status = "unavailable", reason = FleetUnavailableReason(snapshot) }, statusCode: StatusCodes.Status503ServiceUnavailable); return Results.Json(new { state = FleetCompatibilityState(p), channels = p.Channels, channel = new { status = "applied" }, sessionEpoch = snapshot.SessionEpoch, bootId = snapshot.CurrentBootId, connectivity = snapshot.Connectivity, operatingState = p.State, audioGeneration = p.AudioGeneration }); } catch (KeyNotFoundException) { return Results.NotFound(); }
});
app.MapGet("/v2/management/receivers", (HttpContext context) =>
    !security.HasSession(context.Request.Headers.Cookie) || issuer.Role != FleetRole.Admin || lifecycle is null
        ? Results.StatusCode(StatusCodes.Status403Forbidden)
        : Results.Json(lifecycle.List()));
app.MapPost("/v2/management/receivers", async (HttpContext context) =>
{
    if (!Admin(context) || lifecycle is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    ReceiverNameRequest? request = await context.Request.ReadFromJsonAsync<ReceiverNameRequest>(context.RequestAborted); if (request is null) return Results.BadRequest();
    try { (ManagedReceiver receiver, string enrolmentCredential) = lifecycle.Add(request.DisplayName); fleet.Register(receiver.ReceiverId, receiver.DisplayName); return Results.Json(new { receiver, enrolmentCredential }); } catch (InvalidDataException) { return Results.BadRequest(); }
});
app.MapPost("/v2/management/receivers/{receiverId}/rename", async (string receiverId, HttpContext context) =>
{
    if (!Admin(context) || lifecycle is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    ReceiverMutationRequest? request = await context.Request.ReadFromJsonAsync<ReceiverMutationRequest>(context.RequestAborted); if (request is null) return Results.BadRequest();
    try { ManagedReceiver receiver = lifecycle.Rename(receiverId, request.DisplayName ?? "", request.ExpectedRevision); fleet.Rename(receiverId, receiver.DisplayName); return Results.Json(receiver); } catch (KeyNotFoundException) { return Results.NotFound(); } catch (InvalidDataException) { return Results.BadRequest(); } catch (InvalidOperationException) { return Results.Conflict(); }
});
app.MapPost("/v2/management/receivers/{receiverId}/{operation}", async (string receiverId, string operation, HttpContext context) =>
{
    if (!Admin(context) || lifecycle is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    ReceiverMutationRequest? request = await context.Request.ReadFromJsonAsync<ReceiverMutationRequest>(context.RequestAborted); if (request is null) return Results.BadRequest();
    try { ManagedReceiver receiver = operation switch { "disable" => lifecycle.Disable(receiverId, request.ExpectedRevision), "enable" => lifecycle.Enable(receiverId, request.ExpectedRevision), "decommission" => lifecycle.BeginDecommission(receiverId, request.ExpectedRevision), _ => throw new InvalidDataException() }; if (operation == "enable") { fleet.EnableManagedReceiver(receiverId); return Results.Json(receiver); } fleetAudio.Retire(receiverId); fleet.CancelNonStop(receiverId); FleetSnapshot snapshot = fleet.Snapshot(receiverId); if (snapshot.Connectivity != FleetConnectivity.Online || !snapshot.ProjectionCurrent) return Results.Json(lifecycle.RequireRecovery(receiverId)); FleetCommand stop = new(Guid.NewGuid(), receiverId, snapshot.SessionEpoch, snapshot.CurrentBootId!, snapshot.Projection.StateVersion, FleetCommandAction.StopReceiver); if (fleet.Admit(FleetRole.Admin, stop).Outcome != FleetCommandOutcome.Accepted) return Results.Json(lifecycle.RequireRecovery(receiverId)); FleetCommandResult? completion = await fleet.WaitForCompletionAsync(stop, completionTimeout, context.RequestAborted); FleetSafeProjection proof = fleet.Snapshot(receiverId).Projection; if (completion?.Outcome != FleetCommandOutcome.Succeeded || proof.State != ReceiverOperatingState.Stopped || proof.AudioGeneration is not null || !proof.OwnedChildAbsent) return Results.Json(lifecycle.RequireRecovery(receiverId)); receiver = lifecycle.ProveStopped(receiverId); fleet.Retire(receiverId); fleetAudio.Retire(receiverId); if (receiver.LifecycleState == ReceiverLifecycleState.Decommissioned) fleet.RemoveManagedReceiver(receiverId); return Results.Json(receiver); } catch (KeyNotFoundException) { return Results.NotFound(); } catch (InvalidDataException) { return Results.BadRequest(); } catch (InvalidOperationException) { return Results.Conflict(); }
});
app.MapPost("/v2/agent/enrolment/exchange", (HttpContext context) =>
{
    context.Response.Headers.CacheControl = "no-store"; if (lifecycle is null) return Results.NotFound();
    string? authorization = context.Request.Headers["Authorization"].FirstOrDefault();
    if (authorization is null || !authorization.StartsWith("T28-Enrolment ", StringComparison.Ordinal)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    try { (string receiverId, string agentCredential) = lifecycle.Exchange(authorization[14..]); return Results.Json(new { receiverId, agentCredential }); } catch (UnauthorizedAccessException) { return Results.StatusCode(StatusCodes.Status403Forbidden); }
});
app.MapPost("/v2/receivers/{receiverId}/channel", async (string receiverId, HttpContext context) =>
{
    if (!Admin(context)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    FleetScopedChannelRequest? request = await context.Request.ReadFromJsonAsync<FleetScopedChannelRequest>(context.RequestAborted); if (request is null) return Results.BadRequest();
    try { FleetCommand command = new(request.RequestId, receiverId, request.SessionEpoch, request.BootId, request.ExpectedStateVersion, FleetCommandAction.SetChannel, request.ProfileId); FleetCommandResult admitted = fleet.Admit(FleetRole.Admin, command); if (admitted.Outcome != FleetCommandOutcome.Accepted) return FleetResultResponse(admitted); FleetCommandResult? completed = await fleet.WaitForCompletionAsync(command, completionTimeout, context.RequestAborted); return FleetCompletionResponse(command, completed); } catch (KeyNotFoundException) { return Results.NotFound(); }
});
app.MapPost("/v2/receivers/{receiverId}/squelch", async (string receiverId, HttpContext context) =>
{
    if (!Admin(context)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    FleetScopedSquelchRequest? request = await context.Request.ReadFromJsonAsync<FleetScopedSquelchRequest>(context.RequestAborted); if (request is null) return Results.BadRequest();
    try { FleetCommand command = new(request.RequestId, receiverId, request.SessionEpoch, request.BootId, request.ExpectedStateVersion, FleetCommandAction.SetSquelch, ThresholdTenths: request.ThresholdTenths, CtcssTenths: request.CtcssTenths); FleetCommandResult admitted = fleet.Admit(FleetRole.Admin, command); if (admitted.Outcome != FleetCommandOutcome.Accepted) return FleetResultResponse(admitted); FleetCommandResult? completed = await fleet.WaitForCompletionAsync(command, completionTimeout, context.RequestAborted); return FleetCompletionResponse(command, completed); } catch (KeyNotFoundException) { return Results.NotFound(); }
});
app.MapPost("/v2/receivers/{receiverId}/configuration", async (string receiverId, HttpContext context) =>
{
    if (!Admin(context) || !OrdinaryReceiverOperation(lifecycle, receiverId)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    FleetConfigurationRequest? request = await context.Request.ReadFromJsonAsync<FleetConfigurationRequest>(context.RequestAborted);
    if (request is null || string.IsNullOrWhiteSpace(request.ProfileId)) return Results.BadRequest();
    try { return Results.Json(fleet.SaveDesired(FleetRole.Admin, receiverId, request.ProfileId)); } catch (UnauthorizedAccessException) { return Results.StatusCode(StatusCodes.Status403Forbidden); } catch (KeyNotFoundException) { return Results.NotFound(); }
});
app.MapPost("/v2/receivers/{receiverId}/commands/{action}", async (string receiverId, string action, HttpContext context) =>
{
    if (!Admin(context) || !OrdinaryReceiverOperation(lifecycle, receiverId)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    FleetCommandRequest? request = await context.Request.ReadFromJsonAsync<FleetCommandRequest>(context.RequestAborted);
    if (request is null || !Enum.TryParse(action, true, out FleetCommandAction parsed) || parsed is not (FleetCommandAction.StartReceiver or FleetCommandAction.StopReceiver or FleetCommandAction.ApplyConfiguration)) return Results.BadRequest();
    try { FleetCommand command = new(request.RequestId, receiverId, request.SessionEpoch, request.BootId, request.ExpectedStateVersion, parsed, request.ProfileId, request.DesiredRevision); FleetCommandResult admitted = fleet.Admit(FleetRole.Admin, command); if (admitted.Outcome != FleetCommandOutcome.Accepted) return FleetResultResponse(admitted); FleetCommandResult? completed = await fleet.WaitForCompletionAsync(command, completionTimeout, context.RequestAborted); return FleetCompletionResponse(command, completed); } catch (KeyNotFoundException) { return Results.NotFound(); }
});
app.MapPost("/v2/agent/hello", async (HttpContext context) =>
{
    FleetHello? hello = await context.Request.ReadFromJsonAsync<FleetHello>(context.RequestAborted); if (hello is null) return Results.BadRequest();
    if (!FleetIdentity.IsReceiverId(hello.ReceiverId) || !FleetIdentity.IsBootId(hello.BootId) || !FleetIdentity.IsBootId(hello.ConnectionId)) return Results.BadRequest();
    AgentAuthentication? authentication = Agent(context, hello.ReceiverId, true, true); if (authentication is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    try { FleetSession session = fleet.AcceptHello(hello.ReceiverId, hello.BootId, hello.ConnectionId); if (authentication.CredentialStatus == AgentCredentialStatus.PendingProof) pendingProofSessions[session.ReceiverId] = (session, authentication.CredentialId); fleetAudio.Retire(session.ReceiverId); return Results.Json(new { receiverId = session.ReceiverId, bootId = session.BootId, sessionEpoch = session.Epoch, heartbeatIntervalSeconds = 5, snapshot = fleet.Snapshot(session.ReceiverId) }); } catch (Exception error) when (error is InvalidDataException or KeyNotFoundException or InvalidOperationException) { return Results.BadRequest(); }
});
app.MapPost("/v2/agent/heartbeat", async (HttpContext context) =>
{
    FleetAgentEnvelope? message = await context.Request.ReadFromJsonAsync<FleetAgentEnvelope>(context.RequestAborted); if (message is null) return Results.BadRequest();
    // RecoveryOnly exists specifically so the credential-mapped receiver can
    // prove an owned child is absent.  It must retain authenticated liveness;
    // only audio, configuration and Start are excluded from that narrow path.
    if (Agent(context, message.ReceiverId) is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    FleetSession session = new(message.ReceiverId, message.BootId, message.ConnectionId, message.SessionEpoch);
    return fleet.ReceiveHeartbeat(session) ? Results.NoContent() : AgentConflict(context, fleet, session);
});
app.MapPost("/v2/agent/snapshot", async (HttpContext context) =>
{
    FleetAgentSnapshot? message = await context.Request.ReadFromJsonAsync<FleetAgentSnapshot>(context.RequestAborted); if (message is null) return Results.BadRequest();
    AgentAuthentication? authentication = Agent(context, message.ReceiverId, true); if (authentication is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    FleetSession session = new(message.ReceiverId, message.BootId, message.ConnectionId, message.SessionEpoch);
    bool safeAbsent = message.Projection.State == ReceiverOperatingState.Stopped && message.Projection.AudioGeneration is null && message.Projection.OwnedChildAbsent;
    if (authentication.CredentialStatus == AgentCredentialStatus.PendingProof && (!pendingProofSessions.TryGetValue(session.ReceiverId, out var proof) || proof.Session != session || proof.CredentialId != authentication.CredentialId || !safeAbsent)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    if (authentication.RecoveryOnly && !safeAbsent) return Results.StatusCode(StatusCodes.Status403Forbidden);
    if (!fleet.ReceiveSnapshot(session, message.Projection)) return AgentConflict(context, fleet, session);
    fleet.ReconcileRunningIntent(session);
    if (authentication.CredentialStatus == AgentCredentialStatus.PendingProof) { pendingProofSessions.Remove(session.ReceiverId); lifecycle!.ActivatePendingProof(authentication.CredentialId, session.ReceiverId); }
    if (authentication.RecoveryOnly) { ManagedReceiver settled = lifecycle!.ProveStopped(session.ReceiverId); fleet.Retire(session.ReceiverId); if (settled.LifecycleState == ReceiverLifecycleState.Decommissioned) fleet.RemoveManagedReceiver(session.ReceiverId); }
    fleetAudio.Promote(session.ReceiverId, session.Epoch, message.Projection.AudioGeneration);
    return Results.NoContent();
});
app.MapPost("/v2/agent/audio", async (HttpContext context) =>
{
    FleetAgentAudio? message = await context.Request.ReadFromJsonAsync<FleetAgentAudio>(context.RequestAborted);
    if (message is null || message.Pcm.Length != V2Protocol.PcmBytes || string.IsNullOrWhiteSpace(message.AudioGeneration)) return Results.BadRequest();
    if (Agent(context, message.ReceiverId)?.RecoveryOnly != false) return Results.StatusCode(StatusCodes.Status403Forbidden);
    FleetSession session = new(message.ReceiverId, message.BootId, message.ConnectionId, message.SessionEpoch);
    if (!fleet.AcceptAudio(session, message.AudioGeneration)) return AgentConflict(context, fleet, session);
    fleetAudio.Publish(session.ReceiverId, session.Epoch, message.AudioGeneration, message.FirstSample, message.Pcm);
    return Results.NoContent();
});
app.MapGet("/v2/agent/commands", (string receiverId, string bootId, string connectionId, long sessionEpoch, HttpContext context) =>
{
    AgentAuthentication? authentication = Agent(context, receiverId);
    if (authentication is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    try { FleetSession session = new(receiverId, bootId, connectionId, sessionEpoch); if (fleet.SessionStatus(session) != FleetSessionStatus.Current) return AgentConflict(context, fleet, session); IEnumerable<FleetCommand> pending = fleet.Pending(session); if (authentication.RecoveryOnly) pending = pending.Where(command => command.Action == FleetCommandAction.StopReceiver); return Results.Json(pending); } catch (InvalidDataException) { return Results.BadRequest(); }
});
app.MapPost("/v2/agent/acknowledgement", async (HttpContext context) =>
{
    FleetAgentAcknowledgement? message = await context.Request.ReadFromJsonAsync<FleetAgentAcknowledgement>(context.RequestAborted); if (message is null || message.RequestId == Guid.Empty) return Results.BadRequest();
    AgentAuthentication? authentication = Agent(context, message.ReceiverId);
    if (authentication is null) return Results.StatusCode(StatusCodes.Status403Forbidden);
    TaskCompletionSource<bool>? gate; lock (acknowledgementGateLock) gate = acknowledgementGate;
    if (gate is not null) await gate.Task.WaitAsync(context.RequestAborted);
    FleetSession session = new(message.ReceiverId, message.BootId, message.ConnectionId, message.SessionEpoch);
    if (authentication.RecoveryOnly && !fleet.Pending(session).Any(command => command.RequestId == message.RequestId && command.Action == FleetCommandAction.StopReceiver)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    if (!fleet.Complete(session, message.RequestId, message.Outcome, message.Projection)) return AgentConflict(context, fleet, session);
    fleetAudio.Promote(session.ReceiverId, session.Epoch, message.Projection.AudioGeneration);
    return Results.NoContent();
});
app.MapPost("/v2/testing/acknowledgement-hold", (HttpContext context) =>
{
    if (!testControls || !string.Equals(context.Request.Headers["X-T28-V2-Test-Control"], testControlToken, StringComparison.Ordinal)) return Results.NotFound();
    lock (acknowledgementGateLock)
    {
        if (acknowledgementGate is { Task.IsCompleted: false }) return Results.Conflict();
        acknowledgementGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    return Results.NoContent();
});
app.MapPost("/v2/testing/acknowledgement-release", (HttpContext context) =>
{
    if (!testControls || !string.Equals(context.Request.Headers["X-T28-V2-Test-Control"], testControlToken, StringComparison.Ordinal)) return Results.NotFound();
    lock (acknowledgementGateLock) acknowledgementGate?.TrySetResult(true);
    return Results.NoContent();
});
app.MapPost("/v2/testing/receivers/{receiverId}/disconnect", (string receiverId, HttpContext context) =>
{
    if (!testControls || !FleetIdentity.IsReceiverId(receiverId) || !string.Equals(context.Request.Headers["X-T28-V2-Test-Control"], testControlToken, StringComparison.Ordinal)) return Results.NotFound();
    fleet.DisconnectForTesting(receiverId); fleetAudio.Retire(receiverId); return Results.NoContent();
});
app.MapPost("/v2/channel", async (HttpContext context) =>
{
    if (!Admin(context)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    ChannelSwitchRequest? request = await ReadChannelRequestAsync(context.Request, context.RequestAborted);
    if (request is null) return Results.BadRequest(new { status = "rejected", reason = "invalid-request" });
    if (fleetOwner is not null)
    {
        if (request.ExpectedGeneration > long.MaxValue) return Results.BadRequest(new { status = "rejected", reason = "invalid-request" });
        FleetSnapshot current = fleet.Snapshot(defaultReceiverId);
        if (!DefaultFleetCurrent(current) || current.CurrentBootId is null) return Results.Json(new { requestId = request.RequestId, status = "rejected", reason = FleetUnavailableReason(current) }, statusCode: StatusCodes.Status503ServiceUnavailable);
        FleetCommand command = new(request.RequestId, defaultReceiverId, current.SessionEpoch, current.CurrentBootId, (long)request.ExpectedGeneration, FleetCommandAction.SetChannel, request.ProfileId);
        FleetCommandResult admitted = fleet.Admit(FleetRole.Admin, command); if (admitted.Outcome != FleetCommandOutcome.Accepted) return FleetCompatibilityChannelResponse(request.RequestId, admitted);
        FleetCommandResult? completed = await fleet.WaitForCompletionAsync(command, completionTimeout, context.RequestAborted);
        return FleetCompatibilityChannelResponse(request.RequestId, ExactFleetCompletion(command, completed));
    }
    LocalReceiverSession legacy = local!; ReceiverSessionSnapshot before = legacy.Snapshot;
    if (legacy.TryReplayChannel(request.RequestId, out ChannelSwitchResult? replay) && replay is not null) return ChannelResultResponse(replay, before);
    if (before.State is null) return Results.Json(new { requestId = request.RequestId, status = "rejected", reason = "receiver-unavailable" }, statusCode: StatusCodes.Status503ServiceUnavailable);
    if (!before.Channels.Any(channel => channel.ProfileId == request.ProfileId) && !legacy.HasPendingChannel(request.RequestId)) { legacy.ProjectChannelRejection(request.RequestId, "unknown-profile"); return Results.Json(new { requestId = request.RequestId, status = "rejected", reason = "unknown-profile", state = before.State }, statusCode: StatusCodes.Status409Conflict); }
    ChannelSwitchResult? result = await legacy.SetChannelAsync(request, context.RequestAborted);
    ReceiverSessionSnapshot after = legacy.Snapshot;
    if (result is null || after.State is null) { legacy.ProjectChannelRejection(request.RequestId, "receiver-unavailable"); return Results.Json(new { requestId = request.RequestId, status = "rejected", reason = "receiver-unavailable", state = after.State }, statusCode: StatusCodes.Status503ServiceUnavailable); }
    return ChannelResultResponse(result, after);
});
app.MapPost("/v2/squelch", async (HttpContext context) =>
{
    if (!Admin(context)) return Results.StatusCode(StatusCodes.Status403Forbidden);
    SquelchRequest? request = await context.Request.ReadFromJsonAsync<SquelchRequest>(context.RequestAborted);
    if (request is null || request.RequestId == 0 || request.ExpectedGeneration == 0 || request.ExpectedGeneration > long.MaxValue || request.ThresholdTenths is < -1000 or > 0 || !CtcssTone.IsValidTenths(request.CtcssTenths)) return Results.BadRequest();
    if (fleetOwner is not null)
    {
        FleetSnapshot current = fleet.Snapshot(defaultReceiverId);
        if (!DefaultFleetCurrent(current) || current.CurrentBootId is null) return Results.Json(new { requestId = request.RequestId, status = "rejected", reason = FleetUnavailableReason(current) }, statusCode: StatusCodes.Status503ServiceUnavailable);
        FleetCommand command = new(CompatibilityRequestId(request.RequestId), defaultReceiverId, current.SessionEpoch, current.CurrentBootId, (long)request.ExpectedGeneration, FleetCommandAction.SetSquelch, ThresholdTenths: request.ThresholdTenths, CtcssTenths: request.CtcssTenths);
        FleetCommandResult admitted = fleet.Admit(FleetRole.Admin, command); if (admitted.Outcome != FleetCommandOutcome.Accepted) return FleetCompatibilitySquelchResponse(request.RequestId, admitted);
        FleetCommandResult? completed = await fleet.WaitForCompletionAsync(command, completionTimeout, context.RequestAborted);
        return FleetCompatibilitySquelchResponse(request.RequestId, ExactFleetCompletion(command, completed));
    }
    LocalReceiverSession legacy = local!;
    if (!await legacy.SetControlAsync(request.RequestId, request.ExpectedGeneration, request.ThresholdTenths, request.CtcssTenths, context.RequestAborted)) return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    ReceiverControlState? state = legacy.EffectiveState;
    if (state is null || state.ThresholdTenths != request.ThresholdTenths || state.CtcssTenths != request.CtcssTenths) return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    return Results.Json(new { requestId = request.RequestId, thresholdTenths = state.ThresholdTenths, ctcssTenths = state.CtcssTenths, generation = state.Generation });
});
app.Map("/v2/audio", async (HttpContext context) =>
{
    if (!context.WebSockets.IsWebSocketRequest || !security.Accept(context.Request.Headers.Origin, context.Request.Host.Value, context.Request.Headers.Cookie, context.WebSockets.WebSocketRequestedProtocols.ToArray())) { context.Response.StatusCode = StatusCodes.Status403Forbidden; return; }
    string? requestedReceiver = context.Request.Query["receiverId"];
    bool useFleet = fleetOwner is not null || !string.IsNullOrWhiteSpace(requestedReceiver) && requestedReceiver != defaultReceiverId;
    if (useFleet)
    {
        string selectedReceiver = string.IsNullOrWhiteSpace(requestedReceiver) ? defaultReceiverId : requestedReceiver!;
        if (!ListenerVisible(selectedReceiver)) { context.Response.StatusCode = StatusCodes.Status404NotFound; return; }
        FleetSnapshot selected;
        try { selected = fleet.Snapshot(selectedReceiver); } catch (KeyNotFoundException) { context.Response.StatusCode = StatusCodes.Status404NotFound; return; }
        if (!DefaultFleetCurrent(selected) || selected.Projection.State != ReceiverOperatingState.Receiving || selected.Projection.AudioGeneration is not string audioGeneration) { context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable; return; }
        FleetAudioListener source;
        try { source = fleetAudio.Bind(selectedReceiver, selected.SessionEpoch, audioGeneration); } catch (InvalidOperationException) { context.Response.StatusCode = StatusCodes.Status409Conflict; return; }
        BrowserListener fleetBrowser = new(); fleetBrowser.BindGeneration(1);
        using CancellationTokenSource pumpStop = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
        Task pump = Task.Run(async () =>
        {
            try { while (!pumpStop.IsCancellationRequested && !source.Retired) { if (source.TryTake(out (ulong FirstSample, byte[] Pcm) packet)) fleetBrowser.Publish(1, packet.FirstSample, packet.Pcm); else await Task.Delay(2, pumpStop.Token); } }
            catch (OperationCanceledException) when (pumpStop.IsCancellationRequested) { }
            finally { fleetBrowser.RetireGeneration(); }
        }, CancellationToken.None);
        try { using WebSocket socket = await context.WebSockets.AcceptWebSocketAsync(V2Protocol.Subprotocol); await V2WebSocket.RunAsync(socket, fleetBrowser, context.RequestAborted, closeCode => logger.LogInformation("v2 fleet websocket closed {ReceiverId} {CloseCode}", selectedReceiver, closeCode)); }
        finally { pumpStop.Cancel(); await pump; }
        return;
    }
    ReceiverControlState? state = local!.EffectiveState; if (state is null) { context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable; return; }
    BrowserListener listener = new(); listener.BindGeneration(state.Generation); lock (listenersLock) listeners.Add(listener);
    try { using WebSocket socket = await context.WebSockets.AcceptWebSocketAsync(V2Protocol.Subprotocol); await V2WebSocket.RunAsync(socket, listener, context.RequestAborted, closeCode => logger.LogInformation("v2 websocket closed {CloseCode}", closeCode)); }
    finally { lock (listenersLock) listeners.Remove(listener); }
});
app.Run();

static async Task<ChannelSwitchRequest?> ReadChannelRequestAsync(HttpRequest request, CancellationToken token)
{
    if (request.ContentLength is > 512) return null;
    try
    {
        using System.Text.Json.JsonDocument document = await System.Text.Json.JsonDocument.ParseAsync(request.Body, cancellationToken: token);
        System.Text.Json.JsonElement root = document.RootElement;
        if (root.ValueKind != System.Text.Json.JsonValueKind.Object || root.EnumerateObject().Count() != 3 || root.EnumerateObject().Any(property => property.Name is not ("requestId" or "expectedGeneration" or "profileId"))) return null;
        if (!root.TryGetProperty("requestId", out System.Text.Json.JsonElement requestId) || requestId.ValueKind != System.Text.Json.JsonValueKind.String || !Guid.TryParseExact(requestId.GetString(), "D", out Guid parsed) || parsed == Guid.Empty) return null;
        if (!root.TryGetProperty("expectedGeneration", out System.Text.Json.JsonElement generation) || !generation.TryGetUInt64(out ulong expected) || expected == 0) return null;
        if (!root.TryGetProperty("profileId", out System.Text.Json.JsonElement profile) || profile.ValueKind != System.Text.Json.JsonValueKind.String || profile.GetString() is not string profileId) return null;
        _ = LocalBodies.ChannelRequest(new ChannelSwitchRequest(parsed, expected, profileId));
        return new(parsed, expected, profileId);
    }
    catch (Exception error) when (error is System.Text.Json.JsonException or InvalidDataException) { return null; }
}

static IResult ChannelResultResponse(ChannelSwitchResult result, ReceiverSessionSnapshot snapshot)
{
    string status = result.Code == ChannelSwitchCode.Applied ? "applied" : "rejected";
    string? reason = result.Code switch { ChannelSwitchCode.UnknownProfile => "unknown-profile", ChannelSwitchCode.StaleGeneration => "stale-generation", ChannelSwitchCode.ReplacementFailed => "replacement-failed", ChannelSwitchCode.RequestConflict => "request-conflict", _ => null };
    int statusCode = result.Code switch { ChannelSwitchCode.Applied => StatusCodes.Status200OK, ChannelSwitchCode.ReplacementFailed => StatusCodes.Status503ServiceUnavailable, _ => StatusCodes.Status409Conflict };
    ReceiverControlState state = new(result.Generation, result.ThresholdTenths, result.CtcssTenths, result.ProfileId);
    ApprovedChannelProjection? activeChannel = snapshot.Channels.SingleOrDefault(channel => channel.ProfileId == state.ProfileId);
    return Results.Json(new { requestId = result.RequestId, status, reason, state, activeChannel }, statusCode: statusCode);
}
static IResult FleetCompletionResponse(FleetCommand command, FleetCommandResult? result)
{
    FleetCommandResult? exact = ExactFleetCompletion(command, result);
    if (exact is null) return Results.Json(new { requestId = command.RequestId, status = "outcome-unknown", reason = "completion-timeout" }, statusCode: StatusCodes.Status504GatewayTimeout);
    return FleetResultResponse(exact);
}
static IResult FleetResultResponse(FleetCommandResult result)
{
    string status = result.Outcome switch { FleetCommandOutcome.Succeeded => "applied", FleetCommandOutcome.RejectedOffline => "offline", FleetCommandOutcome.RejectedStaleSession => "stale", FleetCommandOutcome.RejectedStaleState => "conflict", FleetCommandOutcome.OutcomeUnknown => "outcome-unknown", FleetCommandOutcome.RequestConflict => "conflict", _ => "rejected" };
    int code = result.Outcome == FleetCommandOutcome.Succeeded ? StatusCodes.Status200OK : result.Outcome == FleetCommandOutcome.RejectedOffline ? StatusCodes.Status503ServiceUnavailable : result.Outcome == FleetCommandOutcome.OutcomeUnknown ? StatusCodes.Status504GatewayTimeout : StatusCodes.Status409Conflict;
    FleetSafeProjection projection = result.Projection;
    return Results.Json(new { requestId = result.RequestId, status, reason = result.Reason, stateVersion = projection.StateVersion, operatingState = projection.State, audioGeneration = projection.AudioGeneration, appliedProfileId = projection.AppliedProfileId, appliedRevision = projection.AppliedRevision, thresholdTenths = projection.ThresholdTenths, ctcssTenths = projection.CtcssTenths }, statusCode: code);
}

static FleetCommandResult? ExactFleetCompletion(FleetCommand command, FleetCommandResult? result)
{
    if (result is null || result.Outcome != FleetCommandOutcome.Succeeded) return result;
    FleetSafeProjection projection = result.Projection;
    bool exact = projection.StateVersion > command.ExpectedStateVersion && (command.Action switch
    {
        FleetCommandAction.StartReceiver => projection.State == ReceiverOperatingState.Receiving && projection.AudioGeneration is not null,
        FleetCommandAction.StopReceiver => projection.State == ReceiverOperatingState.Stopped && projection.AudioGeneration is null,
        FleetCommandAction.ApplyConfiguration => projection.AppliedProfileId == command.ProfileId && projection.AppliedRevision == command.DesiredRevision,
        FleetCommandAction.SetChannel => projection.AppliedProfileId == command.ProfileId,
        FleetCommandAction.SetSquelch => projection.ThresholdTenths == command.ThresholdTenths && projection.CtcssTenths == command.CtcssTenths,
        _ => false
    });
    return exact ? result : result with { Outcome = FleetCommandOutcome.OutcomeUnknown, Reason = "completion-mismatch" };
}
static bool OrdinaryReceiverOperation(ReceiverLifecycleRegistry? lifecycle, string receiverId)
{
    if (lifecycle is null) return true;
    try { return lifecycle.Get(receiverId).LifecycleState == ReceiverLifecycleState.Active; }
    catch (KeyNotFoundException) { return false; }
}
static bool FixedTimeSecretEquals(string actual, string expected) => System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(actual)), System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(expected)));
static bool DefaultFleetCurrent(FleetSnapshot snapshot) => snapshot.Connectivity == FleetConnectivity.Online && snapshot.ProjectionCurrent && snapshot.CurrentBootId is not null;
static string FleetUnavailableReason(FleetSnapshot snapshot) => snapshot.Connectivity switch { FleetConnectivity.Offline => "offline", FleetConnectivity.Stale => "stale-session", _ when !snapshot.ProjectionCurrent => "session-not-synchronized", _ => "receiver-unavailable" };
static object FleetCompatibilityState(FleetSafeProjection projection) => new { generation = projection.StateVersion, stateVersion = projection.StateVersion, thresholdTenths = projection.ThresholdTenths, ctcssTenths = projection.CtcssTenths, profileId = projection.AppliedProfileId, operatingState = projection.State, audioGeneration = projection.AudioGeneration };
static Guid CompatibilityRequestId(ulong requestId)
{
    byte[] value = new byte[16]; System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(value, 0x543238434f4d5002); System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(value.AsSpan(8), requestId); return new(value);
}
static IResult FleetCompatibilityChannelResponse(Guid requestId, FleetCommandResult? result)
{
    if (result is null) return Results.Json(new { requestId, status = "outcome-unknown", reason = "completion-timeout" }, statusCode: StatusCodes.Status504GatewayTimeout);
    if (result.Outcome != FleetCommandOutcome.Succeeded)
    {
        string status = result.Outcome == FleetCommandOutcome.RejectedOffline ? "rejected" : result.Outcome == FleetCommandOutcome.OutcomeUnknown ? "outcome-unknown" : "rejected";
        int code = result.Outcome == FleetCommandOutcome.RejectedOffline ? StatusCodes.Status503ServiceUnavailable : result.Outcome == FleetCommandOutcome.OutcomeUnknown ? StatusCodes.Status504GatewayTimeout : StatusCodes.Status409Conflict;
        return Results.Json(new { requestId, status, reason = result.Reason, state = FleetCompatibilityState(result.Projection) }, statusCode: code);
    }
    FleetSafeProjection projection = result.Projection; ApprovedChannelProjection? activeChannel = projection.Channels?.SingleOrDefault(channel => channel.ProfileId == projection.AppliedProfileId);
    return Results.Json(new { requestId, status = "applied", reason = (string?)null, state = FleetCompatibilityState(projection), activeChannel });
}
static IResult FleetCompatibilitySquelchResponse(ulong requestId, FleetCommandResult? result)
{
    if (result is null) return Results.Json(new { requestId, status = "outcome-unknown", reason = "completion-timeout" }, statusCode: StatusCodes.Status504GatewayTimeout);
    if (result.Outcome != FleetCommandOutcome.Succeeded)
    {
        int code = result.Outcome == FleetCommandOutcome.RejectedOffline ? StatusCodes.Status503ServiceUnavailable : result.Outcome == FleetCommandOutcome.OutcomeUnknown ? StatusCodes.Status504GatewayTimeout : StatusCodes.Status409Conflict;
        return Results.Json(new { requestId, status = "rejected", reason = result.Reason, generation = result.Projection.StateVersion }, statusCode: code);
    }
    FleetSafeProjection projection = result.Projection; return Results.Json(new { requestId, thresholdTenths = projection.ThresholdTenths, ctcssTenths = projection.CtcssTenths, generation = projection.StateVersion });
}
static IResult AgentConflict(HttpContext context, FleetRegistry fleet, FleetSession session)
{
    string fence = fleet.SessionStatus(session) switch { FleetSessionStatus.Replaced => "replaced", FleetSessionStatus.Unknown => "unknown", _ => "current" };
    context.Response.Headers["X-T28-V2-Session-Fence"] = fence; return Results.Json(new { status = "rejected", reason = "session-" + fence }, statusCode: StatusCodes.Status409Conflict);
}

internal sealed record SquelchRequest(ulong RequestId, ulong ExpectedGeneration, short ThresholdTenths, short CtcssTenths);
internal sealed record FleetConfigurationRequest(string ProfileId);
internal sealed record FleetCommandRequest(Guid RequestId, long SessionEpoch, string BootId, long ExpectedStateVersion, string? ProfileId, long? DesiredRevision);
internal sealed record FleetScopedChannelRequest(Guid RequestId, long SessionEpoch, string BootId, long ExpectedStateVersion, string ProfileId);
internal sealed record FleetScopedSquelchRequest(Guid RequestId, long SessionEpoch, string BootId, long ExpectedStateVersion, short ThresholdTenths, short CtcssTenths);
internal sealed record ReceiverNameRequest(string DisplayName);
internal sealed record ReceiverMutationRequest(long ExpectedRevision, string? DisplayName = null);
internal sealed record FleetHello(string ReceiverId, string BootId, string ConnectionId);
internal sealed record FleetAgentEnvelope(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch);
internal sealed record FleetAgentSnapshot(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch, FleetSafeProjection Projection);
internal sealed record FleetAgentAcknowledgement(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch, Guid RequestId, FleetCommandOutcome Outcome, FleetSafeProjection Projection);
internal sealed record FleetAgentAudio(string ReceiverId, string BootId, string ConnectionId, long SessionEpoch, string AudioGeneration, ulong FirstSample, byte[] Pcm);
