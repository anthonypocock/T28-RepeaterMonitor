using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Net.Http.Json;
using System.Text;
using System.Net.Sockets;
using System.Threading.Channels;
using T28.V2.Contracts;
using T28.V2.Receiver;
using T28.V2.Server;

namespace T28.V2.Tests;

internal static class Program
{
    private static async Task Main()
    {
        LocalProtocol(); ChannelContract(); ApprovedChannelCatalogueContract(); FleetConfigurationContract(); FleetAgentTransportContract(); BrowserProtocol(); MorningUiContract(); Security(); SessionAdmission(); Queue(); BrowserControlRecovery(); LiveEdgeSequenceGaps(); FleetContract(); ConfiguredDefaultLifecycleContract(); ReceiverLifecycleContract(); Console.WriteLine("v2 synchronous contract checks passed");
        await FragmentedWebSocketControlsAsync(); await FleetCompletionWaitersAsync(); Console.WriteLine("v2 websocket and completion-wait contract checks passed");
        await QueueContentionAsync(); await SquelchIdentity(); Console.WriteLine("v2 concurrency and control checks passed");
        await ProductionAdapterReadinessAsync(); await ProductionAdapterControlIsolationAsync(); Console.WriteLine("v2 child adapter checks passed");
        await ReceiverProfileLifecycleAsync(); Console.WriteLine("v2 receiver profile lifecycle checks passed");
        await FleetHeartbeatStaleWatchdogAsync(); Console.WriteLine("v2 fleet heartbeat stale watchdog passed");
        await FleetTwoAgentProcessFixtureAsync(); Console.WriteLine("v2 two-agent fleet process fixture passed");
        await LocalSessionLifecycle(); await LocalSessionRetirementRacesAsync(); Console.WriteLine("v2 local session lifecycle checks passed");
        await PhysicalRatePacketOwnershipAsync();
        Console.WriteLine("v2 contract checks passed (synthetic only; not physically verified)");
    }
    private static void Check(bool value, string message) { if (!value) throw new InvalidOperationException(message); }
    private static void FleetContract()
    {
        DateTimeOffset now = new(2026, 8, 4, 8, 0, 0, TimeSpan.Zero); string directory = Path.Combine(Path.GetTempPath(), "t28-v2-fleet-" + Guid.NewGuid().ToString("N")); string path = Path.Combine(directory, "fleet.json");
        try
        {
            FleetRegistry fleet = new(new JsonFleetStore(path), () => now); Check(fleet.Snapshot("lab-demo-hill").Connectivity == FleetConnectivity.Offline, "fleet migration did not default Offline");
            FleetSession alpha = fleet.AcceptHello("lab-demo-hill", "boot-alpha", "connection-alpha"); Check(alpha.Epoch == 1 && fleet.ReceiveHeartbeat(alpha) && fleet.ReceiveSnapshot(alpha, FleetSafeProjection.Stopped("lab-demo-hill")), "authenticated hello/heartbeat/snapshot failed");
            FleetSnapshot desired = fleet.SaveDesired(FleetRole.Admin, "lab-demo-hill", "synthetic-b"); Check(desired.ConfigurationPending && desired.DesiredRevision == 1, "durable desired configuration missing"); FleetSnapshot repeatedDesired = fleet.SaveDesired(FleetRole.Admin, "lab-demo-hill", "synthetic-b"); Check(repeatedDesired.ConfigurationPending && repeatedDesired.DesiredRevision == desired.DesiredRevision, "repeated desired configuration was not idempotent"); FleetCommand blockedStart = new(Guid.NewGuid(), "lab-demo-hill", alpha.Epoch, alpha.BootId, 1, FleetCommandAction.StartReceiver, "synthetic-b", 1); Check(fleet.Admit(FleetRole.Admin, blockedStart).Reason == "configuration-pending" && fleet.Pending(alpha).Count == 0, "pending configuration reached the receiver Start queue");
            FleetCommand apply = new(Guid.NewGuid(), "lab-demo-hill", alpha.Epoch, alpha.BootId, 1, FleetCommandAction.ApplyConfiguration, "synthetic-b", 1); Check(fleet.Admit(FleetRole.Listener, apply).Outcome == FleetCommandOutcome.RejectedUnauthorized && fleet.Admit(FleetRole.Admin, apply).Outcome == FleetCommandOutcome.Accepted, "fleet role fence failed");
            FleetSafeProjection stopped = new(ReceiverOperatingState.Stopped, 2, null, "synthetic-b", 1, "ready", null, OwnedChildAbsent: true); Check(fleet.Complete(alpha, apply.RequestId, FleetCommandOutcome.Succeeded, stopped) && !fleet.SaveDesired(FleetRole.Admin, "lab-demo-hill", "synthetic-b").ConfigurationPending, "stopped apply acknowledgement or no-op save failed");
            FleetSession replacement = fleet.AcceptHello("lab-demo-hill", "boot-alpha", "connection-bravo"); Check(!fleet.ReceiveHeartbeat(alpha), "replacement session retained old connection"); now += TimeSpan.FromSeconds(16); Check(fleet.Snapshot("lab-demo-hill").Connectivity == FleetConnectivity.Stale, "stale threshold failed"); now += TimeSpan.FromSeconds(30); Check(fleet.Snapshot("lab-demo-hill").Connectivity == FleetConnectivity.Offline, "offline threshold failed");
            FleetAudioHub audio = new(); string genA = FleetIdentity.NewAudioGeneration(), genB = FleetIdentity.NewAudioGeneration(); audio.Promote("lab-demo-hill", replacement.Epoch, genA); audio.Promote("receiver-bravo", 1, genB); FleetAudioListener a = audio.Bind("lab-demo-hill", replacement.Epoch, genA), b = audio.Bind("receiver-bravo", 1, genB); audio.Publish("receiver-bravo", 1, genB, 960, new byte[V2Protocol.PcmBytes]); Check(!a.TryTake(out _) && b.TryTake(out _), "receiver audio crossed fan-out boundary"); audio.Promote("receiver-bravo", 1, FleetIdentity.NewAudioGeneration()); Check(b.Retired, "receiver replacement left the old listener live");
            FleetRegistry recovered = new(new JsonFleetStore(path), () => now); Check(recovered.Snapshot("lab-demo-hill").Connectivity == FleetConnectivity.Offline && recovered.Snapshot("lab-demo-hill").DesiredProfileId == "synthetic-b", "recovery restored live state or lost desired configuration");
            string twoReceiverPath = Path.Combine(directory, "two-receivers.json"); JsonFleetStore twoReceiverStore = new(twoReceiverPath); twoReceiverStore.Save([new("lab-demo-hill", "Alpha", true, 0, "synthetic-a", 0, FleetSafeProjection.Stopped("synthetic-a"), null, []), new("receiver-bravo", "Bravo", true, 0, "synthetic-b", 0, FleetSafeProjection.Stopped("synthetic-b"), null, [])]);
            FleetRegistry twoReceivers = new(twoReceiverStore, () => now); FleetSession first = twoReceivers.AcceptHello("lab-demo-hill", "boot-first", "connection-first"), second = twoReceivers.AcceptHello("receiver-bravo", "boot-second", "connection-second"); Check(first.Epoch == 1 && second.Epoch == 1 && twoReceivers.List().Count(snapshot => snapshot.Connectivity == FleetConnectivity.Online) == 2, "two receiver sessions were not isolated and online");
            ApprovedChannelProjection[] alphaChannels = [new("synthetic-a", "371", "Alpha"), new("synthetic-b", "644", "Bravo")], bravoChannels = [new("synthetic-b", "644", "Bravo")];
            Check(twoReceivers.ReceiveSnapshot(first, FleetSafeProjection.Stopped("synthetic-a") with { Channels = alphaChannels }) && twoReceivers.ReceiveSnapshot(second, FleetSafeProjection.Stopped("synthetic-b") with { Channels = bravoChannels }), "receiver control projections were not independently accepted");
            FleetCommand alphaChannel = new(Guid.NewGuid(), first.ReceiverId, first.Epoch, first.BootId, 1, FleetCommandAction.SetChannel, "synthetic-b"); FleetCommand bravoSquelch = new(Guid.NewGuid(), second.ReceiverId, second.Epoch, second.BootId, 1, FleetCommandAction.SetSquelch, ThresholdTenths: -170, CtcssTenths: 1622);
            Check(twoReceivers.Admit(FleetRole.Admin, alphaChannel).Outcome == FleetCommandOutcome.Accepted && twoReceivers.Admit(FleetRole.Admin, bravoSquelch).Outcome == FleetCommandOutcome.Accepted && twoReceivers.Pending(first).Single() == alphaChannel && twoReceivers.Pending(second).Single() == bravoSquelch, "receiver-scoped controls leaked or lost their request fence");
            Check(!twoReceivers.ReceiveHeartbeat(new("lab-demo-hill", second.BootId, second.ConnectionId, second.Epoch)), "cross-receiver envelope was accepted"); FleetCommand start = new(Guid.NewGuid(), first.ReceiverId, first.Epoch, first.BootId, 1, FleetCommandAction.StartReceiver, "synthetic-a", 0); Check(twoReceivers.Admit(FleetRole.Admin, start).Outcome == FleetCommandOutcome.Accepted && twoReceivers.Admit(FleetRole.Admin, start).Outcome == FleetCommandOutcome.Accepted && twoReceivers.Admit(FleetRole.Admin, start with { ExpectedStateVersion = 2 }).Outcome == FleetCommandOutcome.RequestConflict, "idempotent command admission failed");
            FleetSession firstReplacement = twoReceivers.AcceptHello(first.ReceiverId, "boot-replacement", "connection-replacement"); Check(!twoReceivers.ReceiveHeartbeat(first) && twoReceivers.ReceiveHeartbeat(second) && firstReplacement.Epoch == 2, "replacement fenced more than its receiver"); now += TimeSpan.FromSeconds(46); FleetCommand offline = new(Guid.NewGuid(), second.ReceiverId, second.Epoch, second.BootId, 1, FleetCommandAction.StopReceiver); Check(twoReceivers.Admit(FleetRole.Admin, offline).Outcome == FleetCommandOutcome.RejectedOffline, "offline command was retained for replay");
            FleetSession commandSession = twoReceivers.AcceptHello("receiver-bravo", "boot-command", "connection-command"); Check(twoReceivers.ReceiveSnapshot(commandSession, FleetSafeProjection.Stopped("synthetic-b")), "replacement command session did not synchronize"); FleetCommand fenced = new(Guid.NewGuid(), commandSession.ReceiverId, commandSession.Epoch, commandSession.BootId, 1, FleetCommandAction.StopReceiver); Check(twoReceivers.Admit(FleetRole.Admin, fenced).Outcome == FleetCommandOutcome.Accepted && twoReceivers.Pending(commandSession).Single() == fenced, "current-session command was not available to its agent"); Check(twoReceivers.Pending(new("receiver-bravo", commandSession.BootId, "older-connection", commandSession.Epoch)).Count == 0, "cross-connection command was exposed"); Check(twoReceivers.Complete(commandSession, fenced.RequestId, FleetCommandOutcome.Succeeded, FleetSafeProjection.Stopped("synthetic-b", 2) with { StateVersion = 2 }) && twoReceivers.Pending(commandSession).Count == 0, "acknowledged command remained pending");
            string activeGeneration = FleetIdentity.NewAudioGeneration(); FleetSafeProjection receiving = new(ReceiverOperatingState.Receiving, 3, activeGeneration, "synthetic-b", 2, "ready", null); Check(twoReceivers.ReceiveSnapshot(commandSession, receiving) && twoReceivers.AcceptAudio(commandSession, activeGeneration) && !twoReceivers.AcceptAudio(firstReplacement, activeGeneration) && !twoReceivers.AcceptAudio(commandSession, FleetIdentity.NewAudioGeneration()), "generation-bound audio session fence failed");             Check(twoReceivers.ReceiveSnapshot(commandSession, FleetSafeProjection.Stopped("synthetic-b", 2) with { StateVersion = 4 }) && !twoReceivers.AcceptAudio(commandSession, activeGeneration), "stopped receiver retained audio generation");
            FleetRegistry intent = new(new JsonFleetStore(Path.Combine(directory, "intent.json")), () => now);
            FleetSession intentSession = intent.AcceptHello("lab-demo-hill", "boot-intent", "connection-intent");
            Check(intent.ReceiveSnapshot(intentSession, FleetSafeProjection.Stopped("lab-demo-hill")), "intent snapshot failed");
            intent.ReconcileRunningIntent(intentSession);
            Check(intent.Pending(intentSession).Single().Action == FleetCommandAction.StartReceiver, "WantsRunning did not enqueue Start after snapshot");
            FleetSafeProjection receivingIntent = new(ReceiverOperatingState.Receiving, 2, FleetIdentity.NewAudioGeneration(), "lab-demo-hill", 0, "ready", null);
            FleetCommand autoStart = intent.Pending(intentSession).Single();
            Check(intent.Complete(intentSession, autoStart.RequestId, FleetCommandOutcome.Succeeded, receivingIntent), "auto Start completion failed");
            string intentPath = Path.Combine(directory, "intent.json");
            Check(intent.Snapshot("lab-demo-hill").DesiredRunning && new JsonFleetStore(intentPath).Load().Single(record => record.ReceiverId == "lab-demo-hill").DesiredRunning == true, "Start did not persist DesiredRunning");
            intentSession = intent.AcceptHello("lab-demo-hill", "boot-stop", "connection-stop");
            Check(intent.ReceiveSnapshot(intentSession, receivingIntent with { StateVersion = 2 }), "stop-intent snapshot failed");
            FleetCommand stopIntent = new(Guid.NewGuid(), intentSession.ReceiverId, intentSession.Epoch, intentSession.BootId, 2, FleetCommandAction.StopReceiver);
            Check(intent.Admit(FleetRole.Admin, stopIntent).Outcome == FleetCommandOutcome.Accepted, "Stop was not admitted");
            Check(intent.Complete(intentSession, stopIntent.RequestId, FleetCommandOutcome.Succeeded, FleetSafeProjection.Stopped("lab-demo-hill", 0) with { StateVersion = 3, OwnedChildAbsent = true }), "Stop completion failed");
            Check(!intent.Snapshot("lab-demo-hill").DesiredRunning && new JsonFleetStore(intentPath).Load().Single(record => record.ReceiverId == "lab-demo-hill").DesiredRunning == false, "Stop did not clear DesiredRunning");
            intentSession = intent.AcceptHello("lab-demo-hill", "boot-idle", "connection-idle");
            Check(intent.ReceiveSnapshot(intentSession, FleetSafeProjection.Stopped("lab-demo-hill", 0) with { StateVersion = 4, OwnedChildAbsent = true }), "idle snapshot failed");
            intent.ReconcileRunningIntent(intentSession);
            Check(intent.Pending(intentSession).Count == 0, "cleared DesiredRunning still enqueued Start");
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }
    private static void ReceiverLifecycleContract()
    {
        DateTimeOffset now = new(2026, 8, 5, 0, 0, 0, TimeSpan.Zero); string directory = Path.Combine(Path.GetTempPath(), "t28-v2-lifecycle-" + Guid.NewGuid().ToString("N")); string path = Path.Combine(directory, "lifecycle.json");
        try
        {
            ReceiverLifecycleRegistry registry = new(new JsonReceiverLifecycleStore(path), Enumerable.Repeat((byte)7, 32).ToArray(), clock: () => now);
            (ManagedReceiver alpha, string enrolment) = registry.Add("Alpha receiver");
            Check(alpha.ReceiverId.StartsWith("rxr_", StringComparison.Ordinal) && alpha.LifecycleState == ReceiverLifecycleState.PendingEnrolment && enrolment.StartsWith("t28e1.", StringComparison.Ordinal), "add did not issue an opaque one-time enrolment credential");
            string persisted = File.ReadAllText(path); Check(!persisted.Contains(enrolment, StringComparison.Ordinal) && !persisted.Contains("Alpha receiver", StringComparison.Ordinal) == false, "durable lifecycle store retained plaintext enrolment credential");
            (string receiverId, string agentCredential) = registry.Exchange(enrolment); Check(receiverId == alpha.ReceiverId && agentCredential.StartsWith("t28a1.", StringComparison.Ordinal), "enrolment exchange did not bind a durable credential to its receiver");
            string beforeWrongCredential = File.ReadAllText(path), wrongCredential = agentCredential[..^1] + (agentCredential[^1] == 'A' ? "B" : "A"); now += TimeSpan.FromMinutes(16); try { registry.Authenticate(wrongCredential, true); throw new InvalidOperationException("wrong same-handle credential was accepted"); } catch (UnauthorizedAccessException) { } Check(File.ReadAllText(path) == beforeWrongCredential, "wrong agent credential mutated durable status"); now -= TimeSpan.FromMinutes(16);
            try { registry.Exchange(enrolment); throw new InvalidOperationException("enrolment replay was accepted"); } catch (UnauthorizedAccessException) { }
            try { registry.Authenticate(agentCredential); throw new InvalidOperationException("pending credential authenticated before Hello/snapshot proof"); } catch (UnauthorizedAccessException) { }
            AgentAuthentication pending = registry.Authenticate(agentCredential, true); Check(pending.ReceiverId == alpha.ReceiverId && pending.CredentialStatus == AgentCredentialStatus.PendingProof && registry.Get(alpha.ReceiverId).LifecycleState == ReceiverLifecycleState.PendingEnrolment, "pending proof changed state before full snapshot");
            ManagedReceiver activated = registry.ActivatePendingProof(pending.CredentialId, alpha.ReceiverId); Check(activated.ReceiverId == alpha.ReceiverId && activated.LifecycleState == ReceiverLifecycleState.Active && activated.CredentialEpoch == 2, "first agent proof did not atomically activate after Hello/snapshot proof");
            try { registry.Authenticate(agentCredential.Replace("t28a1.", "t28a1.x", StringComparison.Ordinal)); throw new InvalidOperationException("malformed credential was accepted"); } catch (UnauthorizedAccessException) { }
            ManagedReceiver renamed = registry.Rename(alpha.ReceiverId, "Alpha renamed", activated.ManagementRevision); Check(renamed.DisplayName == "Alpha renamed" && renamed.ManagementRevision == activated.ManagementRevision + 1, "receiver rename did not use its management revision fence");
            try { registry.Rename(alpha.ReceiverId, "stale", activated.ManagementRevision); throw new InvalidOperationException("stale management revision was accepted"); } catch (InvalidOperationException) { }
            ManagedReceiver disabled = registry.Disable(alpha.ReceiverId, renamed.ManagementRevision); Check(disabled.LifecycleState == ReceiverLifecycleState.DisablePendingStop && registry.Authenticate(agentCredential).RecoveryOnly, "disable revoked the only recovery credential before stop proof");
            disabled = registry.ProveStopped(alpha.ReceiverId); Check(disabled.LifecycleState == ReceiverLifecycleState.Disabled, "current stop proof did not settle disable");
            try { registry.Authenticate(agentCredential); throw new InvalidOperationException("disabled receiver accepted an ordinary agent session"); } catch (UnauthorizedAccessException) { }
            ManagedReceiver enabled = registry.Enable(alpha.ReceiverId, disabled.ManagementRevision); Check(enabled.LifecycleState == ReceiverLifecycleState.Active && enabled.ManagementRevision == disabled.ManagementRevision + 1 && registry.Authenticate(agentCredential).CredentialStatus == AgentCredentialStatus.Active, "enable did not retain the disabled receiver credential verifier");
            (ManagedReceiver bravo, string bravoOffer) = registry.Add("Bravo receiver"); (string bravoId, string bravoCredential) = registry.Exchange(bravoOffer); AgentAuthentication bravoPending = registry.Authenticate(bravoCredential, true); ManagedReceiver bravoActive = registry.ActivatePendingProof(bravoPending.CredentialId, bravoId); Check(bravoId == bravo.ReceiverId && registry.Get(alpha.ReceiverId).DisplayName == "Alpha renamed" && bravoActive.ReceiverId != alpha.ReceiverId, "one receiver lifecycle operation mutated another receiver");
            string replacementOffer = registry.IssueReplacement(bravoId); (_, string replacementCredential) = registry.Exchange(replacementOffer); AgentAuthentication replacementPending = registry.Authenticate(replacementCredential, true); Check(registry.Authenticate(bravoCredential).CredentialStatus == AgentCredentialStatus.Active, "replacement pending proof revoked the prior recovery path"); bravoActive = registry.ActivatePendingProof(replacementPending.CredentialId, bravoId); try { registry.Authenticate(bravoCredential); throw new InvalidOperationException("proved replacement retained older credential"); } catch (UnauthorizedAccessException) { }
            (ManagedReceiver expired, string expiredOffer) = registry.Add("Expired receiver"); now += TimeSpan.FromMinutes(11); try { registry.Exchange(expiredOffer); throw new InvalidOperationException("expired enrolment was accepted"); } catch (UnauthorizedAccessException) { }
            ManagedReceiver pendingRemoval = registry.BeginDecommission(bravo.ReceiverId, bravoActive.ManagementRevision); Check(pendingRemoval.LifecycleState == ReceiverLifecycleState.DecommissionPendingStop && registry.Get(alpha.ReceiverId).LifecycleState == ReceiverLifecycleState.Active, "decommission preparation crossed receiver boundaries");
            Check(registry.Authenticate(replacementCredential).RecoveryOnly, "decommission revoked the recovery path before proof");
            ManagedReceiver removed = registry.ProveStopped(bravo.ReceiverId); Check(removed.LifecycleState == ReceiverLifecycleState.Decommissioned && removed.RestoreUntil == now.AddDays(30) && removed.TombstoneExpiresAt == now.AddDays(365) && !registry.List().Any(value => value.ReceiverId == bravo.ReceiverId), "decommission did not leave distinct restore and tombstone deadlines");
            ReceiverLifecycleRegistry reloaded = new(new JsonReceiverLifecycleStore(path), Enumerable.Repeat((byte)7, 32).ToArray(), clock: () => now); Check(reloaded.Get(alpha.ReceiverId).DisplayName == "Alpha renamed" && reloaded.Get(bravo.ReceiverId).LifecycleState == ReceiverLifecycleState.Decommissioned, "durable lifecycle state did not safely rehydrate");
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }
    private static void ConfiguredDefaultLifecycleContract()
    {
        DateTimeOffset now = new(2026, 8, 5, 1, 0, 0, TimeSpan.Zero); string directory = Path.Combine(Path.GetTempPath(), "t28-v2-default-lifecycle-" + Guid.NewGuid().ToString("N")); string path = Path.Combine(directory, "lifecycle.json"); byte[] pepper = Enumerable.Repeat((byte)9, 32).ToArray(); const string receiverId = "lab-demo-hill", token = "synthetic-default-owner-token";
        try
        {
            ReceiverLifecycleRegistry registry = new(new JsonReceiverLifecycleStore(path), pepper, clock: () => now);
            Check(registry.AuthenticateConfiguredDefault(receiverId, "Demo Hill lab receiver", "synthetic-wrong-owner-token", receiverId, token) is null &&
                registry.AuthenticateConfiguredDefault("receiver-other", "Other receiver", token, receiverId, token) is null &&
                registry.AuthenticateConfiguredDefault(receiverId, "Demo Hill lab receiver", token, receiverId, null) is null &&
                registry.AuthenticateConfiguredDefault(receiverId, "Demo Hill lab receiver", token, receiverId, token) is null &&
                !File.Exists(path), "invalid configured-default authentication caused durable lifecycle mutation");

            string occupiedPath = Path.Combine(directory, "occupied.json"); ReceiverLifecycleRegistry occupied = new(new JsonReceiverLifecycleStore(occupiedPath), pepper, clock: () => now); (ManagedReceiver prior, _) = occupied.Add("Prior managed receiver");
            Check(occupied.AuthenticateConfiguredDefault(receiverId, "Demo Hill lab receiver", token, receiverId, token, true) is null && occupied.List().Single().ReceiverId == prior.ReceiverId, "configured-default activation mutated a nonempty lifecycle store");

            ConcurrentBag<AgentAuthentication?> activations = [];
            Parallel.For(0, 8, _ => activations.Add(registry.AuthenticateConfiguredDefault(receiverId, "Demo Hill lab receiver", token, receiverId, token, true)));
            Check(activations.All(value => value is { ReceiverId: receiverId, CredentialStatus: AgentCredentialStatus.Active, LifecycleState: ReceiverLifecycleState.Active }), "configured default did not activate idempotently");
            ReceiverLifecycleDocument document = new JsonReceiverLifecycleStore(path).Load(); string persisted = File.ReadAllText(path);
            Check(document.Receivers is [{ ReceiverId: receiverId, LifecycleState: ReceiverLifecycleState.Active }] && document.Credentials.Length == 1 && document.Offers.Length == 0 && !persisted.Contains(token, StringComparison.Ordinal) && !persisted.Contains(Convert.ToHexString(pepper), StringComparison.Ordinal), "configured-default activation did not persist one secret-free Active lifecycle record");

            ReceiverLifecycleRegistry reloaded = new(new JsonReceiverLifecycleStore(path), pepper, clock: () => now);
            AgentAuthentication? restarted = reloaded.AuthenticateConfiguredDefault(receiverId, "Demo Hill lab receiver", token, receiverId, token);
            Check(restarted is { ReceiverId: receiverId, CredentialStatus: AgentCredentialStatus.Active } && reloaded.List().Count == 1, "configured default did not reconnect from one durable lifecycle record");

            ManagedReceiver active = reloaded.Get(receiverId); ManagedReceiver disabling = reloaded.Disable(receiverId, active.ManagementRevision);
            Check(disabling.LifecycleState == ReceiverLifecycleState.DisablePendingStop && reloaded.AuthenticateConfiguredDefault(receiverId, active.DisplayName, token, receiverId, token)?.RecoveryOnly == true, "configured default bypassed disable recovery fencing");
            ManagedReceiver disabled = reloaded.ProveStopped(receiverId); Check(reloaded.AuthenticateConfiguredDefault(receiverId, active.DisplayName, token, receiverId, token) is null, "configured default authenticated while Disabled");
            ManagedReceiver enabled = reloaded.Enable(receiverId, disabled.ManagementRevision); Check(reloaded.AuthenticateConfiguredDefault(receiverId, enabled.DisplayName, token, receiverId, token)?.RecoveryOnly == false, "re-enabled configured default did not retain its owner-local credential verifier");

            string offer = reloaded.IssueReplacement(receiverId); (_, string ordinaryCredential) = reloaded.Exchange(offer);
            Check(reloaded.AuthenticateConfiguredDefault(receiverId, enabled.DisplayName, token, receiverId, token)?.CredentialStatus == AgentCredentialStatus.Active, "pending replacement changed the existing credential semantics");
            AgentAuthentication pending = reloaded.Authenticate(ordinaryCredential, true); ManagedReceiver ordinaryActive = reloaded.ActivatePendingProof(pending.CredentialId, receiverId);
            Check(reloaded.Authenticate(ordinaryCredential).CredentialStatus == AgentCredentialStatus.Active && reloaded.AuthenticateConfiguredDefault(receiverId, ordinaryActive.DisplayName, token, receiverId, token) is null, "ordinary ADR-005 activation retained the compatibility path");
            ManagedReceiver decommissioning = reloaded.BeginDecommission(receiverId, ordinaryActive.ManagementRevision); Check(reloaded.Authenticate(ordinaryCredential).RecoveryOnly, "ordinary credential lost its bounded decommission recovery path");
            _ = reloaded.ProveStopped(receiverId); try { reloaded.Authenticate(ordinaryCredential); throw new InvalidOperationException("decommissioned ordinary credential authenticated"); } catch (UnauthorizedAccessException) { }
            Check(reloaded.AuthenticateConfiguredDefault(receiverId, ordinaryActive.DisplayName, token, receiverId, token) is null && reloaded.List().Count == 0, "configured default bypassed decommission fencing");
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }
    private static async Task FleetCompletionWaitersAsync()
    {
        string directory = Path.Combine(Path.GetTempPath(), "t28-v2-waiters-" + Guid.NewGuid().ToString("N"));
        try
        {
            FleetRegistry fleet = new(new JsonFleetStore(Path.Combine(directory, "fleet.json")));
            FleetSession session = fleet.AcceptHello("lab-demo-hill", "boot-waiter", "connection-waiter"); Check(fleet.ReceiveSnapshot(session, FleetSafeProjection.Stopped("lab-demo-hill")), "completion waiter session did not synchronize");
            FleetCommand command = new(Guid.NewGuid(), session.ReceiverId, session.Epoch, session.BootId, 1, FleetCommandAction.SetSquelch, ThresholdTenths: -170, CtcssTenths: 1622);
            Check(fleet.Admit(FleetRole.Admin, command).Outcome == FleetCommandOutcome.Accepted, "completion waiter command was not admitted");
            FleetSafeProjection applied = FleetSafeProjection.Stopped("lab-demo-hill") with { StateVersion = 2, ThresholdTenths = -170, CtcssTenths = 1622 };
            Check(fleet.Complete(session, command.RequestId, FleetCommandOutcome.Succeeded, applied), "acknowledgement-before-wait was not retained");
            Check((await fleet.WaitForCompletionAsync(command, TimeSpan.FromMilliseconds(50), CancellationToken.None))?.Outcome == FleetCommandOutcome.Succeeded, "acknowledgement-before-wait lost exact completion");
            FleetCommand waiting = command with { RequestId = Guid.NewGuid(), ExpectedStateVersion = 2, ThresholdTenths = -180 };
            Check(fleet.Admit(FleetRole.Admin, waiting).Outcome == FleetCommandOutcome.Accepted, "waiter command was not admitted");
            Task<FleetCommandResult?> pending = fleet.WaitForCompletionAsync(waiting, TimeSpan.FromSeconds(1), CancellationToken.None);
            await Task.Delay(10); Check(!pending.IsCompleted, "admission was incorrectly reported as completion");
            Check(fleet.Complete(session, waiting.RequestId, FleetCommandOutcome.Succeeded, applied with { StateVersion = 3, ThresholdTenths = -180 }), "matching acknowledgement did not complete waiter");
            Check((await pending)?.Projection.ThresholdTenths == -180, "waiter completed with another receiver/control projection");
            FleetCommand expired = waiting with { RequestId = Guid.NewGuid(), ExpectedStateVersion = 3, ThresholdTenths = -190 };
            Check(fleet.Admit(FleetRole.Admin, expired).Outcome == FleetCommandOutcome.Accepted, "timeout waiter command was not admitted");
            Check(await fleet.WaitForCompletionAsync(expired, TimeSpan.FromMilliseconds(10), CancellationToken.None) is null && !fleet.Pending(session).Contains(expired), "timed-out command remained available for replay");
            Check(!fleet.Complete(session, expired.RequestId, FleetCommandOutcome.Succeeded, applied with { StateVersion = 4, ThresholdTenths = -190 }) && fleet.Admit(FleetRole.Admin, expired).Outcome == FleetCommandOutcome.OutcomeUnknown, "late acknowledgement or duplicate revived timed-out command");
            FleetCommand cancelled = waiting with { RequestId = Guid.NewGuid(), ExpectedStateVersion = 3 };
            Check(fleet.Admit(FleetRole.Admin, cancelled).Outcome == FleetCommandOutcome.Accepted, "replacement waiter command was not admitted");
            Task<FleetCommandResult?> replacementWait = fleet.WaitForCompletionAsync(cancelled, TimeSpan.FromSeconds(1), CancellationToken.None);
            _ = fleet.AcceptHello("lab-demo-hill", "boot-replaced", "connection-replaced");
            Check(await replacementWait is null, "session replacement did not cancel waiter");
            Check(await fleet.WaitForCompletionAsync(cancelled, TimeSpan.FromMilliseconds(10), CancellationToken.None) is null, "replaced command remained available for replay");
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }
    private static void LocalProtocol()
    {
        byte[] wire = V2Protocol.Frame(LocalType.Hello, LocalBodies.Hello()); Check(V2Protocol.ParseLocal(wire).Type == LocalType.Hello, "hello rejected");
        wire[4] = 1; try { V2Protocol.ParseLocal(wire); throw new InvalidOperationException("bad version accepted"); } catch (InvalidDataException) { }
    }
    private static void ChannelContract()
    {
        ApprovedChannelProjection[] channels = [new("lab-demo-hill", "371", "Demo Hill"), new("synthetic-b", "TST", "Synthetic B")];
        byte[] registry = LocalBodies.ChannelRegistry(channels); IReadOnlyList<ApprovedChannelProjection> projected = LocalBodies.ChannelRegistry(registry);
        Check(projected.Count == 2 && projected[0].FriendlyName == "371 - Demo Hill" && projected.All(channel => channel.GetType().GetProperties().All(property => property.Name is not ("ProfilePath" or "AdapterPath" or "Executable"))), "friendly registry projection leaked or changed identity");
        Guid id = Guid.Parse("11111111-2222-3333-4444-555555555555"); ChannelSwitchRequest request = new(id, 7, "synthetic-b");
        Check(LocalBodies.ChannelRequest(LocalBodies.ChannelRequest(request)) == request, "channel request identity did not round trip");
        ChannelSwitchResult result = new(id, ChannelSwitchCode.Applied, 8, "synthetic-b", -1000, 1622);
        Check(LocalBodies.ChannelResult(LocalBodies.ChannelResult(result)) == result, "channel result did not retain generation and acknowledged controls");
        _ = V2Protocol.ParseLocal(V2Protocol.Frame(LocalType.ChannelRegistry, registry));
    }
    private static void ApprovedChannelCatalogueContract()
    {
        IReadOnlyList<ApprovedChannelDefinition> all = ApprovedChannelCatalogue.All;
        Check(all.Count == 241 && all.Select(value => value.PlanName).Where(value => value.Contains(' ')).Distinct().Count() >= 32, "catalogue did not retain every plan membership");
        (string Plan, string Number, long Frequency)[] commands = [("Demo location 27","247",145750000),("Demo location 28","294",145337500),("Demo location 29","133",145500000),("Demo location 30","153",145537500),("Demo location 31","224",145312500),("Demo location 32","261",145300000),("Demo location 33","346",145850000),("Demo location 34","110",145787500),("Demo location 35","112",145462500),("Demo location 14","231",145625000),("Demo location 15","173",145400000),("Demo location 16","316",145475000),("Demo location 17","229",145562500),("Demo location 18","138",145725000),("Demo location 19","121",145487500),("Demo location 20","142",145812500),("Demo location 21","260",145600000),("Demo location 22","206",145775000),("Demo location 23","238",145362500),("Demo location 24","141",145575000),("Demo location 25","144",145637500),("Demo location 26","128",145450000),("Demo location 36","109",145675000),("Demo location 37","234",145387500),("Demo location 38","168",145325000),("Demo location 39","161",145350000),("Demo location 40","221",145862500),("Demo location 41","218",145825000),("Demo location 42","139",145662500),("Demo location 43","170",145550000)];
        foreach ((string plan, string number, long frequency) in commands) Check(all.Single(value => value.PlanName == plan && value.Number == number).FrequencyHz == frequency, "command receive frequency changed");
        (string Plan, int FirstSector)[] expectedSectorRanges = [("Demo location 27",354),("Demo location 28",354),("Demo location 29",360),("Demo location 30",354),("Demo location 31",360),("Demo location 32",354),("Demo location 33",360),("Demo location 34",354),("Demo location 35",360),("Demo location 14",354),("Demo location 15",360),("Demo location 16",354),("Demo location 17",360),("Demo location 18",354),("Demo location 19",360),("Demo location 20",360),("Demo location 21",354),("Demo location 22",360),("Demo location 23",354),("Demo location 24",354),("Demo location 25",354),("Demo location 26",360),("Demo location 36",354),("Demo location 37",360),("Demo location 38",354),("Demo location 39",354),("Demo location 40",360),("Demo location 41",354),("Demo location 42",360),("Demo location 43",354)];
        Check(expectedSectorRanges.Length == commands.Length, "sector test did not cover every incident plan");
        foreach ((string plan, int firstSector) in expectedSectorRanges) Check(all.Where(value => value.PlanName == plan && int.TryParse(value.Number, out int number) && number is >= 354 and <= 365).Select(value => value.Number).Order().SequenceEqual(Enumerable.Range(firstSector, 6).Select(value => value.ToString())), "plan sector membership changed");
        long[] sectors = [145225000,145237500,145275000,145262500,145150000,145137500,145287500,145162500,145175000,145200000,145187500,145212500];
        for (int index = 0; index < sectors.Length; index++) Check(all.Where(value => value.Number == (354 + index).ToString()).All(value => value.FrequencyHz == sectors[index]), "sector receive frequency changed");
        (string Number, string Name, long Frequency)[] networkControl = [("6","Demo location 13",145037500),("24","Demo location 4",145075000),("25","Demo location 11",145087500),("30","Demo location 3",145112500),("52","Demo location 8",145000000),("53","Demo location 61",145012500),("55","Demo Hill",145025000),("56","Demo location 54",145037500),("57","Demo location 55",145050000),("58","Demo location 65",145062500),("77","Demo location 60",145100000),("80","Demo location 12",145112500),("92","Demo location 10",145125000),("105","Demo location 3",145425000),("205","Demo location 9",145412500),("207","Demo location 11",145650000),("217","Demo location 54",145687500),("220","Demo location 12",145700000),("246","Demo location 13",145612500),("263","Demo location 60",145375000),("317","Demo location 10",145800000),("328","Demo location 4",145587500),("371","Demo Hill",145512500),("373","Demo location 8",145525000),("376","Demo location 65",145737500),("377","Demo location 61",145762500),("379","Demo location 5",145837500),("380","Demo location 55",145875000)];
        ApprovedChannelDefinition[] projectedNetworkControl = all.Where(value => value.PlanId == "network-control").ToArray();
        Check(projectedNetworkControl.Select(value => (value.Number, value.DisplayName, value.FrequencyHz)).SequenceEqual(networkControl), "Demo location 52 disagrees with the committed JSON/map authority");
        Check(projectedNetworkControl.Select(value => int.Parse(value.Number)).SequenceEqual(projectedNetworkControl.Select(value => int.Parse(value.Number)).Order()), "Demo location 52 selector order is not numeric");
        Check(all.Single(value => value.PlanId == "shared-air-operations" && value.Number == "644").FrequencyHz == 145712500 && all.Single(value => value.PlanId == "shared-air-operations" && value.Number == "621").FrequencyHz == 145437500 && all.Single(value => value.PlanId == "shared-air-operations" && value.Number == "369").FrequencyHz == 145250000, "shared approved receive frequency changed");
        Check(all.Count(value => value.Number == "354") > 1 && all.Select(value => (value.PlanId, value.Number)).Distinct().Count() == all.Count, "overlapping sector membership became ambiguous");
        RegistryLoadRejectsForgedIdentity();
    }
    private static void FleetConfigurationContract()
    {
        Check(FleetAgentConfiguration.Parse(null, null, null) is null, "absent fleet configuration did not select legacy-local compatibility");
        FleetAgentConfiguration configured = FleetAgentConfiguration.Parse("https://fleet.test/v2", "lab-demo-hill", "synthetic-task-token") ?? throw new InvalidOperationException("fleet configuration was not accepted");
        Check(configured.ServerUrl == new Uri("https://fleet.test/v2") && configured.ReceiverId == "lab-demo-hill" && configured.AgentToken == "synthetic-task-token", "fleet owner seam changed configured values");
        foreach ((string? url, string? receiver, string? token) in new[]
        {
            ("https://fleet.test", null, "synthetic-task-token"),
            (null, "lab-demo-hill", "synthetic-task-token"),
            ("file:///tmp/fleet", "lab-demo-hill", "synthetic-task-token"),
            ("https://fleet.test", "bad", "synthetic-task-token")
        })
        {
            try { _ = FleetAgentConfiguration.Parse(url, receiver, token); throw new InvalidOperationException("partial or unsafe fleet configuration was accepted"); }
            catch (InvalidDataException) { }
        }
    }
    private static void FleetAgentTransportContract()
    {
        Check(FleetReceiverAgent.ResolveHeartbeatStaleTimeout(null) == TimeSpan.FromMinutes(15), "absent heartbeat-stale minutes did not default to 15");
        Check(FleetReceiverAgent.ResolveHeartbeatStaleTimeout("9") == TimeSpan.FromMinutes(15), "heartbeat-stale minutes below 10 were not rejected");
        Check(FleetReceiverAgent.ResolveHeartbeatStaleTimeout("10") == TimeSpan.FromMinutes(10), "minimum heartbeat-stale minutes were not honored");
        Check(FleetReceiverAgent.ResolveHeartbeatStaleTimeout("20") == TimeSpan.FromMinutes(20), "configured heartbeat-stale minutes were not honored");
        Check(FleetReceiverAgent.ResolveHeartbeatStaleTimeout("bogus") == TimeSpan.FromMinutes(15), "invalid heartbeat-stale minutes did not fall back to 15");
        Check(FleetReceiverAgent.ControlPlaneRequestTimeout("/v2/agent/heartbeat") == TimeSpan.FromSeconds(30), "control-plane timeout was not 30s");
        Check(FleetReceiverAgent.ControlPlaneRequestTimeout("/v2/agent/commands?waitMilliseconds=5000") == TimeSpan.FromSeconds(10), "long-poll waitMilliseconds slack was not honored");
        Check(FleetReceiverAgent.ControlPlaneRequestTimeout("/v2/agent/commands?receiverId=lab-demo-hill&waitMilliseconds=2500&sessionEpoch=1") == TimeSpan.FromSeconds(7.5), "embedded waitMilliseconds slack was not honored");
    }
    private static async Task FleetHeartbeatStaleWatchdogAsync()
    {
        ReceiverChildInputs inputs = new("synthetic", "/synthetic/adapter", "/synthetic/profile", Synthetic: true);
        ApprovedReceiverProfile profile = new("lab-demo-hill", "371", "Demo Hill", inputs);
        ReceiverProfileRegistry registry = new([profile], profile.ProfileId);
        TimeSpan stale = TimeSpan.FromMilliseconds(300);
        using HangControlHandler hang = new(hangHeartbeat: true);
        using HttpClient hungClient = new(hang) { BaseAddress = new Uri("https://fleet.test"), Timeout = Timeout.InfiniteTimeSpan };
        int hungExit = 0;
        await using FleetReceiverAgent hungAgent = new(registry, new LifecycleFactory(), hungClient, profile.ProfileId, "synthetic-agent-token", TimeSpan.FromMilliseconds(25), stale, code => Interlocked.Exchange(ref hungExit, code));
        using CancellationTokenSource hungStop = new(TimeSpan.FromSeconds(3));
        Task hungRun = hungAgent.RunAsync(hungStop.Token);
        Stopwatch hungElapsed = Stopwatch.StartNew();
        while (Volatile.Read(ref hungExit) == 0 && hungElapsed.Elapsed < TimeSpan.FromSeconds(2)) await Task.Delay(20);
        Check(Volatile.Read(ref hungExit) == 1, "hung heartbeat with infinite HttpClient timeout did not request a non-zero process exit");
        hungStop.Cancel(); await SuppressCanceledAsync(hungRun);

        using HangControlHandler live = new(hangHeartbeat: false);
        using HttpClient liveClient = new(live) { BaseAddress = new Uri("https://fleet.test"), Timeout = TimeSpan.FromSeconds(30) };
        int liveExit = 0;
        await using FleetReceiverAgent liveAgent = new(registry, new LifecycleFactory(), liveClient, profile.ProfileId, "synthetic-agent-token", TimeSpan.FromMilliseconds(25), TimeSpan.FromMilliseconds(400), code => Interlocked.Exchange(ref liveExit, code));
        using CancellationTokenSource liveStop = new();
        Task liveRun = liveAgent.RunAsync(liveStop.Token);
        await Task.Delay(700);
        liveStop.Cancel(); await SuppressCanceledAsync(liveRun);
        Check(Volatile.Read(ref liveExit) == 0 && live.Heartbeats > 0, "successful heartbeats still requested a stale-heartbeat process exit");
    }
    private static void RegistryLoadRejectsForgedIdentity()
    {
        string directory = Path.Combine(Path.GetTempPath(), "t28-v2-catalogue-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            string adapter = Path.Combine(directory, "adapter.py"), profile = Path.Combine(directory, "profile.json"), registry = Path.Combine(directory, "registry.json"); File.WriteAllText(adapter, "# fixture");
            void Write(long frequency, string displayName, string planId = "network-control", string planName = "Demo location 52", string planGroup = "Other approved categories")
            {
                File.WriteAllText(profile, System.Text.Json.JsonSerializer.Serialize(new { profile_id = "opaque-371", frequency_hz = frequency, channel_bandwidth_hz = 12500 }));
                File.WriteAllText(registry, System.Text.Json.JsonSerializer.Serialize(new { activeProfileId = "opaque-371", profiles = new[] { new { profileId = "opaque-371", number = "371", displayName, planId, planName, planGroup, executable = "python3", adapterPath = adapter, profilePath = profile, synthetic = true } } }));
            }
            Write(145512500, "Demo Hill"); Check(ReceiverProfileRegistry.Load(registry).Active.Projection.FriendlyName == "371 - Demo Hill", "registry did not derive canonical active identity");
            (long frequency, string displayName, string planId, string planName, string planGroup)[] forgedValues = [(163675001,"Demo Hill","network-control","Demo location 52","Other approved categories"),(145512500,"Stale workbook label","network-control","Demo location 52","Other approved categories"),(145512500,"Demo Hill","shared-air-operations","Demo location 58","Other approved categories"),(145512500,"Demo Hill","network-control","Forged browser label","Other approved categories")];
            foreach ((long frequency, string displayName, string planId, string planName, string planGroup) forged in forgedValues)
            {
                Write(forged.frequency, forged.displayName, forged.planId, forged.planName, forged.planGroup); try { _ = ReceiverProfileRegistry.Load(registry); throw new InvalidOperationException("forged registry identity was accepted"); } catch (InvalidDataException) { }
            }
            List<object> completeProfiles = [];
            foreach (ApprovedChannelDefinition definition in ApprovedChannelCatalogue.All.Where(value => value.PlanId == "network-control"))
            {
                string profileId = "opaque-network-" + definition.Number, profilePath = Path.Combine(directory, profileId + ".json");
                File.WriteAllText(profilePath, System.Text.Json.JsonSerializer.Serialize(new { profile_id = profileId, frequency_hz = definition.FrequencyHz, channel_bandwidth_hz = 12500 }));
                completeProfiles.Add(new { profileId, number = definition.Number, displayName = definition.DisplayName, planId = definition.PlanId, planName = definition.PlanName, planGroup = definition.PlanGroup, executable = "python3", adapterPath = adapter, profilePath, synthetic = true });
            }
            File.WriteAllText(registry, System.Text.Json.JsonSerializer.Serialize(new { activeProfileId = "opaque-network-371", profiles = completeProfiles }));
            ReceiverProfileRegistry complete = ReceiverProfileRegistry.Load(registry);
            Check(complete.Active.Projection.FriendlyName == "371 - Demo Hill" && complete.PublicProfiles.Count == 28 && complete.PublicProfiles.Select(profile => profile.ProfileId).Distinct().Count() == 28, "complete Demo location 52 registry did not retain unique opaque profiles");
        }
        finally { Directory.Delete(directory, true); }
    }
    private static void BrowserProtocol()
    {
        byte[] packet = V2Protocol.BrowserPacket(7, 1, 960, new byte[V2Protocol.PcmBytes]); Check(V2Protocol.ParseBrowser(packet) is { Attempt: 7, Sequence: 1 }, "packet rejected");
        packet[39] = 2; try { V2Protocol.ParseBrowser(packet); throw new InvalidOperationException("bad packet accepted"); } catch (InvalidDataException) { }
        ClientMessage played = V2Protocol.ParseClient("{\"type\":\"played\",\"attempt\":1,\"sequence\":2}"u8);
        Check(played.Played == 2 && played.Paused is null, "played parsed as flow control");
        ClientMessage consume = V2Protocol.ParseClient("{\"type\":\"consume\",\"attempt\":1,\"sequence\":2,\"packets\":1}"u8);
        Check(consume.Played == 2 && consume.Credit == 1 && consume.IsConsume, "atomic consume control rejected");
        Check(V2Protocol.ParseClient("{\"type\":\"played\",\"attempt\":1,\"sequence\":9007199254740991}"u8).Played == 9_007_199_254_740_991, "exact browser-safe played acknowledgement rejected");
        Check(V2Protocol.ParseClient("{\"type\":\"credit\",\"attempt\":1,\"packets\":4}"u8).Credit == 4, "startup credit rejected");
        try { V2Protocol.ParseClient("{\"type\":\"credit\",\"attempt\":0,\"packets\":5}"u8); throw new InvalidOperationException("invalid credit accepted"); } catch (InvalidDataException) { }
    }
    private static void MorningUiContract()
    {
        string listener = Portal.ListenerIndexHtml;
        string listenerReceiver = Portal.Page("listener", fleet: false, "null");
        string admin = Portal.Page("admin", fleet: true, "null");
        string[] operatorChrome = ["Channel controls", "Apply settings", "Start receiver", "Stop receiver", "Add receiver", "Remove receiver", "Disable receiver", "fleet-sidebar", "Create enrolment"];
        Check(listener.Contains("<title>Listener</title>", StringComparison.Ordinal)
            && listener.Contains("data-page=\"listener-index\"", StringComparison.Ordinal)
            && listener.Contains("fetch('/v2/session?view=listener'", StringComparison.Ordinal)
            && listener.Contains("new EventSource('/v2/events?view=listener')", StringComparison.Ordinal)
            && operatorChrome.All(needle => !listener.Contains(needle, StringComparison.Ordinal)),
            "GET /listener HTML is not the 201df8ba listen-only index");
        Check(listenerReceiver.Contains("<title>Listener</title>", StringComparison.Ordinal)
            && listenerReceiver.Contains("data-page=listener", StringComparison.Ordinal)
            && listenerReceiver.Contains("id=play", StringComparison.Ordinal)
            && !listenerReceiver.Contains("fleet-sidebar", StringComparison.Ordinal)
            && !listenerReceiver.Contains("Add receiver", StringComparison.Ordinal),
            "GET /listener/receiver is not the 201df8ba listen widget");
        Check(admin.Contains("<title>Admin</title>", StringComparison.Ordinal)
            && admin.Contains("data-page=admin", StringComparison.Ordinal)
            && admin.Contains("Channel controls", StringComparison.Ordinal)
            && admin.Contains("Apply settings", StringComparison.Ordinal)
            && admin.Contains("fleet-sidebar", StringComparison.Ordinal)
            && admin.Contains("Add receiver", StringComparison.Ordinal)
            && admin.Contains("Start receiver", StringComparison.Ordinal)
            && admin.Contains("id=\"receiver-start\"", StringComparison.Ordinal)
            && admin.Contains("body[data-page=admin] .listening-controls,body[data-page=admin] #status{display:none}", StringComparison.Ordinal),
            "GET /admin HTML is not the 201df8ba channel-control and fleet document");
    }
    private static void Security()
    {
        V2BrowserSecurity policy = new("https://private.example", "private.example", "t28_v2_session", "owner");
        Check(policy.Accept("https://private.example", "private.example", "t28_v2_session=owner", [V2Protocol.Subprotocol]), "valid upgrade rejected");
        Check(!policy.Accept("https://other", "private.example", "t28_v2_session=owner", [V2Protocol.Subprotocol]), "foreign origin accepted");
        Check(!policy.Accept("https://private.example", "private.example", "t28_v2_session=owner", ["wrong"]), "wrong protocol accepted");
        Check(!policy.Accept("https://private.example", "private.example", "t28_v2_session=owner", [V2Protocol.Subprotocol, "other"]), "multiple protocols accepted");
        Check(!policy.Accept("https://private.example", "private.example", null, [V2Protocol.Subprotocol]), "missing cookie accepted");
        Check(!policy.Accept("https://private.example", "other.example", "t28_v2_session=owner", [V2Protocol.Subprotocol]), "wrong host accepted");
    }
    private static void SessionAdmission()
    {
        V2SessionIssuer issuer = new("https://private.example", "private.example"); V2BrowserSecurity security = new(issuer.Origin, issuer.Host, "t28_v2_session", issuer.Session);
        Check(security.MatchesOriginHost("https://private.example", "private.example"), "valid session admission rejected");
        Check(!security.MatchesOriginHost(null, "private.example") && !security.MatchesOriginHost("https://private.example", null), "missing origin or host accepted");
        Check(!security.MatchesOriginHost("https://wrong.example", "private.example") && !security.MatchesOriginHost("https://private.example", "wrong.example"), "wrong origin or host accepted");
        V2SessionIssuer listener = new("https://private.example", "private.example", FleetRole.Listener);
        Check(listener.Role == FleetRole.Listener && !listener.CheckCsrf("wrong") && listener.CheckCsrf(listener.Csrf), "listener session role or csrf fence changed");
        Check(security.HasSession("other=x; t28_v2_session=" + issuer.Session) && issuer.CheckCsrf(issuer.Csrf), "issued bootstrap credentials rejected");
        Check(!security.HasSession("t28_v2_session=wrong") && !issuer.CheckCsrf("wrong") && !issuer.CheckCsrf(null), "wrong cookie or csrf accepted");
    }
    private static void Queue()
    {
        LiveQueue<int> queue = new(); foreach (int i in Enumerable.Range(1, 6)) queue.Push(i); Check(queue.TryTake(out int first) && first == 2, "queue did not drop live edge");
        BrowserListener listener = new(); listener.BindGeneration(1); listener.Publish(1, 0, new byte[V2Protocol.PcmBytes]); Check(!listener.TrySend(out _), "paused listener sent PCM");
        Check(listener.Control(new ClientMessage(1, false, null, 0)) == BrowserControlOutcome.Accepted && listener.Control(new ClientMessage(1, null, null, 4)) == BrowserControlOutcome.Accepted, "startup flow/credit rejected");
        for (int i = 0; i < 5; i++) listener.Publish(1, (ulong)i * 960, new byte[V2Protocol.PcmBytes]); for (int i = 0; i < 4; i++) Check(listener.TrySend(out _), "credited packet not sent");
        // Source publication continues at its bounded live edge while the
        // worklet consumes the prime.  A played acknowledgement retires only
        // sent media, so the first returned credit must remain valid.
        listener.Publish(1, 5_760, new byte[V2Protocol.PcmBytes]);
        Check(listener.Control(new ClientMessage(1, null, 1, 1, true)) == BrowserControlOutcome.Accepted && listener.TrySend(out _), "source publication incorrectly exhausted browser credit");
        Check(listener.Control(new ClientMessage(1, null, null, 2)) == BrowserControlOutcome.IgnoredExcessiveCredit, "isolated excessive credit was not safely ignored"); Check(listener.Control(new ClientMessage(1, true, null, 0)) == BrowserControlOutcome.Accepted && !listener.TrySend(out _), "paused listener retained PCM");
        Check(listener.Control(new ClientMessage(1, null, 99, 0)) == BrowserControlOutcome.StaleAttempt, "stale attempt taxonomy missing");
        BrowserListener sequence = new(); Check(sequence.Control(new ClientMessage(1, false, null, 0)) == BrowserControlOutcome.Accepted && sequence.Control(new ClientMessage(1, null, 1, 0)) == BrowserControlOutcome.IgnoredInvalidPlayedSequence, "invalid played sequence taxonomy missing");
    }
    private static void BrowserControlRecovery()
    {
        BrowserListener listener = new(); listener.BindGeneration(1);
        Check(listener.Control(new ClientMessage(7, false, null, 0)) == BrowserControlOutcome.Accepted && listener.Control(new ClientMessage(7, null, null, 4)) == BrowserControlOutcome.Accepted, "recovery test startup rejected");
        for (ulong index = 1; index <= 5; index++) listener.Publish(1, index * V2Protocol.SamplesPerPacket, new byte[V2Protocol.PcmBytes]);
        List<ulong> sent = [];
        for (int index = 0; index < 4; index++) { Check(listener.TrySend(out byte[]? packet) && packet is not null, "recovery test packet missing"); Check(BinaryPrimitives.ReadUInt64BigEndian(packet.AsSpan(8)) == 7, "packet lost its owning attempt"); sent.Add(BinaryPrimitives.ReadUInt64BigEndian(packet.AsSpan(16))); }
        Check(listener.Control(new ClientMessage(7, null, sent[2], 1, true)) == BrowserControlOutcome.Accepted, "cumulative exact acknowledgement and replacement failed");
        Check(listener.Credit == 1, "exact consume did not grant one bounded replacement");
        Check(listener.Control(new ClientMessage(7, null, sent[0], 1, true)) == BrowserControlOutcome.Accepted, "already-retired duplicate consume was not harmless");
        Check(listener.Credit == 1, "duplicate consume granted a second replacement");
        Check(listener.Control(new ClientMessage(7, null, sent[3] + 1, 0)) == BrowserControlOutcome.IgnoredInvalidPlayedSequence, "future acknowledgement was not counted");
        Check(listener.Control(new ClientMessage(7, null, sent[3], 1, true)) == BrowserControlOutcome.Accepted, "valid consume did not reset anomaly budget");
        Check(listener.Credit <= 4 && listener.QueueDepth <= LiveQueue<BrowserPacket>.Capacity && !listener.Lagging, "recovery test weakened bounded state");
        for (int index = 0; index < 3; index++) Check(listener.Control(new ClientMessage(7, null, null, 1)) == BrowserControlOutcome.IgnoredExcessiveCredit, "isolated duplicate credit did not remain recoverable");
        Check(listener.Control(new ClientMessage(7, null, null, 1)) == BrowserControlOutcome.ExcessiveCredit, "persistent excess credit did not fail closed");
        BrowserListener fabricated = new(); Check(fabricated.Control(new ClientMessage(3, false, null, 0)) == BrowserControlOutcome.Accepted, "fabricated test startup rejected");
        for (int index = 0; index < 4; index++) Check(fabricated.Control(new ClientMessage(3, null, 123, 0)) == (index < 3 ? BrowserControlOutcome.IgnoredInvalidPlayedSequence : BrowserControlOutcome.InvalidPlayedSequence), "persistent fabricated acknowledgement taxonomy failed");
        Check(fabricated.Control(new ClientMessage(2, null, 123, 0)) == BrowserControlOutcome.StaleAttempt && fabricated.Control(new ClientMessage(3, null, 123, 0)) == BrowserControlOutcome.InvalidPlayedSequence, "stale controls consumed the current anomaly budget");
    }
    private static void LiveEdgeSequenceGaps()
    {
        // Retire bounded live-edge frames between every consumer acknowledgement.
        // The browser sees ordered sent packets with large source-sequence gaps;
        // only those exact sent sequence numbers may acknowledge credit.
        BrowserListener listener = new(); listener.BindGeneration(1); List<ulong> sent = [];
        Check(listener.Control(new ClientMessage(1, false, null, 0)) == BrowserControlOutcome.Accepted && listener.Control(new ClientMessage(1, null, null, 4)) == BrowserControlOutcome.Accepted, "gap test startup flow/credit rejected");
        for (ulong first = 1; first <= 5; first++) listener.Publish(1, first * V2Protocol.SamplesPerPacket, new byte[V2Protocol.PcmBytes]);
        for (int index = 0; index < 4; index++) { Check(listener.TrySend(out byte[]? packet) && packet is not null, "gap test initial sent packet missing"); sent.Add(BinaryPrimitives.ReadUInt64BigEndian(packet.AsSpan(16))); }
        for (int cycle = 0; cycle < 80; cycle++)
        {
            for (ulong retired = 0; retired < 5; retired++) listener.Publish(1, ((ulong)(cycle * 5 + 6) + retired) * V2Protocol.SamplesPerPacket, new byte[V2Protocol.PcmBytes]);
            ulong played = sent[cycle];
            Check(listener.Control(new ClientMessage(1, null, played, 1, true)) == BrowserControlOutcome.Accepted, "atomic sent acknowledgement failed after live-edge retirement");
            Check(listener.TrySend(out byte[]? packet) && packet is not null, "valid returned credit did not send a live-edge packet"); sent.Add(BinaryPrimitives.ReadUInt64BigEndian(packet.AsSpan(16)));
            Check(listener.QueueDepth <= LiveQueue<BrowserPacket>.Capacity && listener.Credit <= 4 && !listener.Lagging, "gap test weakened queue or credit bounds");
        }
        Check(sent.Zip(sent.Skip(1), (left, right) => right > left).All(value => value) && sent.Zip(sent.Skip(1), (left, right) => right - left > 1).Any(value => value), "gap test did not force ordered source-sequence gaps");
        ulong next = sent[80];
        Check(listener.Control(new ClientMessage(1, null, sent[83] + 1, 0)) == BrowserControlOutcome.IgnoredInvalidPlayedSequence, "future fabricated acknowledgement was not counted");
        Check(listener.Control(new ClientMessage(1, null, sent[79], 0)) == BrowserControlOutcome.Accepted, "already-retired duplicate acknowledgement was rejected");
        Check(listener.Control(new ClientMessage(1, null, next, 0)) == BrowserControlOutcome.Accepted, "next exact acknowledgement was rejected after invalid controls");
        Check(listener.Control(new ClientMessage(1, null, sent[82], 0)) == BrowserControlOutcome.Accepted, "later cumulative acknowledgement was rejected");
        Check(listener.Control(new ClientMessage(2, null, next, 0)) == BrowserControlOutcome.StaleAttempt, "older-attempt callback was not harmless");
    }
    private static async Task FragmentedWebSocketControlsAsync()
    {
        BrowserListener listener = new(); listener.BindGeneration(1); FragmentSocket socket = new(); List<V2WebSocket.CloseCode> close = [];
        socket.AddText("{\"type\":\"flow\",\"attempt\":1,\"paused\":false}", oneByte: true);
        socket.AddText("{\"type\":\"credit\",\"attempt\":1,\"packets\":4}", oneByte: true);
        bool primed = false;
        socket.BeforeReceive = () =>
        {
            if (primed || listener.Paused) return;
            primed = true;
            for (int index = 0; index < 4; index++) listener.Publish(1, (ulong)index * V2Protocol.SamplesPerPacket, new byte[V2Protocol.PcmBytes]);
        };
        socket.BinarySent = sequence =>
        {
            listener.Publish(1, sequence * V2Protocol.SamplesPerPacket, new byte[V2Protocol.PcmBytes]);
            socket.AddText($"{{\"type\":\"consume\",\"attempt\":1,\"sequence\":{sequence},\"packets\":1}}", splitAt: 1);
            if (sequence == 3_004) socket.AddClose();
        };
        await V2WebSocket.RunAsync(socket, listener, CancellationToken.None, close.Add);
        Check(close.SequenceEqual([V2WebSocket.CloseCode.PeerClosed]) && socket.CloseStatus is null, "complete fragmented controls did not remain open until the peer close");
        Check(socket.BinarySequences.Count >= 3_004 && socket.BinarySequences.SequenceEqual(Enumerable.Range(1, socket.BinarySequences.Count).Select(index => (ulong)index)), "fragmented atomic consume cadence lost or reordered a packet");
        Check(listener.QueueDepth <= 4 && listener.Credit <= 4 && !listener.Lagging, "fragmented controls weakened the live queue or consumer-credit bounds");

        await RejectControlAsync(socket => socket.AddBinary([1], true), V2WebSocket.CloseCode.MalformedFraming, "binary control was accepted");
        await RejectControlAsync(socket => socket.AddText(string.Empty), V2WebSocket.CloseCode.MalformedFraming, "empty control was accepted");
        await RejectControlAsync(socket => socket.AddText(new string('x', 128), endOfMessage: false), V2WebSocket.CloseCode.MalformedFraming, "oversized fragmented control was accepted");
        await RejectControlAsync(socket => { socket.AddTextBytes([0xc3], false); socket.AddTextBytes([0x28], true); }, V2WebSocket.CloseCode.MalformedFraming, "split invalid UTF-8 was accepted");
        await RejectControlAsync(socket => { socket.AddTextBytes(Encoding.UTF8.GetBytes("{\"type\":\"flow\""), false); socket.AddBinary([0], true); }, V2WebSocket.CloseCode.MalformedFraming, "interleaved control types were accepted");
        await RejectControlAsync(socket => { socket.AddText("{\"type\":\"flow\",\"attempt\":1,\"paused\":false}"); socket.AddText("{\"type\":\"credit\",\"attempt\":1,\"packets\":4}"); for (int index = 0; index < 4; index++) socket.AddText("{\"type\":\"credit\",\"attempt\":1,\"packets\":1}"); }, V2WebSocket.CloseCode.ExcessiveCredit, "persistent excessive credit taxonomy missing");

        FragmentSocket stale = new(); List<V2WebSocket.CloseCode> staleClose = [];
        stale.AddText("{\"type\":\"flow\",\"attempt\":2,\"paused\":false}"); stale.AddText("{\"type\":\"credit\",\"attempt\":2,\"packets\":4}");
        stale.AddText("{\"type\":\"played\",\"attempt\":1,\"sequence\":1}"); stale.AddText("{\"type\":\"credit\",\"attempt\":1,\"packets\":1}"); stale.AddClose();
        await V2WebSocket.RunAsync(stale, new BrowserListener(), CancellationToken.None, staleClose.Add);
        Check(staleClose.SequenceEqual([V2WebSocket.CloseCode.PeerClosed]) && stale.CloseStatus is null, "stale attempt control closed the current socket");

        FragmentSocket peerClose = new(); List<V2WebSocket.CloseCode> peerCloseCode = []; peerClose.AddTextBytes(Encoding.UTF8.GetBytes("{\"type\":\"flow\""), false); peerClose.AddClose();
        await V2WebSocket.RunAsync(peerClose, new BrowserListener(), CancellationToken.None, peerCloseCode.Add);
        Check(peerCloseCode.SequenceEqual([V2WebSocket.CloseCode.PeerClosed]) && peerClose.CloseStatus is null, "close frame was not categorised without processing a partial control");
    }
    private static async Task RejectControlAsync(Action<FragmentSocket> arrange, V2WebSocket.CloseCode expected, string message)
    {
        FragmentSocket socket = new(); List<V2WebSocket.CloseCode> close = []; arrange(socket);
        await V2WebSocket.RunAsync(socket, new BrowserListener(), CancellationToken.None, close.Add);
        string reason = expected switch { V2WebSocket.CloseCode.MalformedFraming => "malformed-control", V2WebSocket.CloseCode.InvalidPlayedSequence => "invalid-played", V2WebSocket.CloseCode.ExcessiveCredit => "excess-credit", _ => throw new InvalidOperationException("unexpected policy close") };
        Check(close.SequenceEqual([expected]) && socket.CloseStatus == WebSocketCloseStatus.PolicyViolation && socket.CloseStatusDescription == reason, message);
    }
    private static async Task QueueContentionAsync()
    {
        // Accelerated repeated simultaneous producer/consumer/clear/count
        // operations exercise the exact four public LiveQueue operations.
        for (int round = 0; round < 40; round++)
        {
            LiveQueue<int> queue = new(); ConcurrentDictionary<int, byte> taken = new(); int invalid = 0; int producers = 4;
            using CancellationTokenSource stop = new();
            Task[] writers = Enumerable.Range(0, producers).Select(worker => Task.Run(() =>
            {
                for (int index = 1; index <= 2_000; index++) queue.Push(worker * 10_000 + index);
            })).ToArray();
            Task[] readers = Enumerable.Range(0, 3).Select(_ => Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    _ = queue.Count;
                    if (queue.TryTake(out int value))
                    {
                        if (value <= 0 || !taken.TryAdd(value, 0)) Interlocked.Exchange(ref invalid, 1);
                    }
                }
            })).ToArray();
            Task clearer = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested) { queue.Clear(); _ = queue.Count; }
            });
            await Task.WhenAll(writers); stop.Cancel(); await Task.WhenAll(readers.Append(clearer));
            while (queue.TryTake(out int value)) if (value <= 0 || !taken.TryAdd(value, 0)) invalid = 1;
            Check(invalid == 0 && queue.Count <= LiveQueue<int>.Capacity, "concurrent live queue corrupted or exceeded capacity");
        }
    }
    private static async Task SquelchIdentity()
    {
        FakeChild child = new(); V2Receiver receiver = new(child); int id = receiver.ChildProcessId;
        Check(receiver.Threshold == -1000 && receiver.CtcssTenths == 1622, "new receiver did not expose the requested fallback");
        Check(await receiver.SetThresholdAsync(1, -170, TimeSpan.FromMilliseconds(100), CancellationToken.None), "initial squelch failed"); child.Timeout = true;
        Check(!await receiver.SetThresholdAsync(2, -200, TimeSpan.FromMilliseconds(5), CancellationToken.None), "timeout accepted"); Check(receiver.ChildProcessId == id && receiver.Threshold == -170, "threshold changed lifecycle identity");
        child.Timeout = false; Check(await receiver.SetControlAsync(3, -170, CtcssTone.None, TimeSpan.FromMilliseconds(100), CancellationToken.None) && receiver.CtcssTenths == CtcssTone.None, "Manual None was not acknowledged in place");
        Check(!await receiver.SetControlAsync(4, -170, 1621, TimeSpan.FromMilliseconds(100), CancellationToken.None) && receiver.CtcssTenths == CtcssTone.None, "invalid CTCSS selection changed effective state");
    }
    private static async Task ProductionAdapterControlIsolationAsync()
    {
        string adapter = Path.GetFullPath("v2/gnuradio/t28_v2_receive.py"), profile = Path.GetFullPath("v2/README.md");
        await VerifyAdapterAsync(adapter, profile, "valid", diagnostics: true, firstExpected: true, secondExpected: true, retained: -190);
        await VerifyAdapterAsync(adapter, profile, "malformed", diagnostics: false, firstExpected: false, secondExpected: null, retained: -1000);
        await VerifyAdapterAsync(adapter, profile, "mismatch", diagnostics: false, firstExpected: false, secondExpected: null, retained: -1000);
        await VerifyAdapterAsync(adapter, profile, "timeout", diagnostics: false, firstExpected: false, secondExpected: null, retained: -1000);
        await VerifyAdapterAsync(adapter, profile, "eof", diagnostics: false, firstExpected: false, secondExpected: null, retained: -1000);
        await VerifyAdapterAsync(adapter, profile, "out-of-order", diagnostics: false, firstExpected: false, secondExpected: null, retained: -1000);
        await VerifyAdapterAsync(adapter, profile, "duplicate", diagnostics: false, firstExpected: true, secondExpected: false, retained: -180);
    }
    private static async Task ProductionAdapterReadinessAsync()
    {
        string adapter = Path.GetFullPath("v2/gnuradio/t28_v2_receive.py"), profile = Path.GetFullPath("v2/README.md");
        GnuRadioProcessAdapter delayed = new("python3", adapter, profile, synthetic: true, syntheticReadinessMode: "delayed");
        using CancellationTokenSource pcmStop = new(); IAsyncEnumerator<(ulong FirstSample, byte[] Pcm)> pcm = delayed.ReadPcmAsync(pcmStop.Token).GetAsyncEnumerator(); Task<bool> firstPcm = pcm.MoveNextAsync().AsTask(); Stopwatch elapsed = Stopwatch.StartNew();
        try
        {
            await Task.Delay(250); Check(!firstPcm.IsCompleted, "PCM was released before the explicit readiness marker");
            using CancellationTokenSource deadline = new(TimeSpan.FromSeconds(3)); await delayed.ReadyAsync(deadline.Token); elapsed.Stop();
            Check(elapsed.Elapsed > TimeSpan.FromSeconds(1), "delayed explicit readiness did not exceed the former one-second PCM gate");
            Check(await delayed.SetControlAsync(91, -1000, 1622, CancellationToken.None) == (-1000, 1622), "exact control acknowledgement did not follow readiness");
        }
        finally
        {
            pcmStop.Cancel(); await delayed.StopAsync(CancellationToken.None); try { await firstPcm; } catch (OperationCanceledException) { } await pcm.DisposeAsync();
        }

        foreach (string mode in new[] { "malformed", "control", "eof" }) await CheckRejectedReadinessAsync(adapter, profile, mode, TimeSpan.FromSeconds(2));
        foreach (string mode in new[] { "timeout", "late" }) await CheckRejectedReadinessAsync(adapter, profile, mode, TimeSpan.FromMilliseconds(250));
        GnuRadioProcessAdapter duplicate = new("python3", adapter, profile, synthetic: true, syntheticReadinessMode: "duplicate");
        try
        {
            await duplicate.ReadyAsync(CancellationToken.None);
            try { await duplicate.SetControlAsync(92, -1000, 1622, CancellationToken.None); throw new InvalidOperationException("duplicate readiness marker was consumed as an exact control acknowledgement"); }
            catch (InvalidDataException) { }
        }
        finally { await duplicate.StopAsync(CancellationToken.None); }
    }
    private static async Task CheckRejectedReadinessAsync(string adapter, string profile, string mode, TimeSpan timeout)
    {
        GnuRadioProcessAdapter child = new("python3", adapter, profile, synthetic: true, syntheticReadinessMode: mode);
        try
        {
            using CancellationTokenSource deadline = new(timeout);
            try { await child.ReadyAsync(deadline.Token); throw new InvalidOperationException($"{mode} readiness marker was accepted"); }
            catch (InvalidDataException) { }
            catch (EndOfStreamException) { }
            catch (OperationCanceledException) when (mode is "timeout" or "late") { }
        }
        finally { await child.StopAsync(CancellationToken.None); }
    }
    private static async Task ReceiverProfileLifecycleAsync()
    {
        ReceiverChildInputs inputs = new("synthetic", "/synthetic/adapter", "/synthetic/profile", Synthetic: true);
        ApprovedReceiverProfile firstProfile = new("lab-demo-hill", "371", "Demo Hill", inputs);
        ApprovedReceiverProfile secondProfile = new("synthetic-b", "644", "Demo location 62", inputs, PlanId: "shared-air-operations", PlanName: "Demo location 58", PlanGroup: "Other approved categories");
        ReceiverProfileRegistry registry = new([firstProfile, secondProfile], firstProfile.ProfileId); LifecycleFactory factory = new();
        await using V2ReceiverOwner owner = new(registry, factory); await owner.InitializeAsync(CancellationToken.None);
        Check(owner.PublicProfiles.Single(profile => profile.ProfileId == firstProfile.ProfileId).FriendlyName == "371 - Demo Hill", "owner did not project the authoritative current identity");
        Check(owner.State == new ReceiverOwnedState(1, firstProfile.ProfileId, -1000, 1622) && factory.Created.Count == 1, "initial approved child was not ready with v2 defaults");

        Guid unknownId = Guid.Parse("10000000-0000-0000-0000-000000000001");
        ChannelSwitchResult unknown = await owner.SwitchAsync(new(unknownId, 1, "not-approved"), CancellationToken.None);
        Check(unknown.Code == ChannelSwitchCode.UnknownProfile && unknown.Generation == 1 && factory.Created.Count == 1, "unknown profile mutated receiver ownership");
        Guid staleId = Guid.Parse("10000000-0000-0000-0000-000000000002");
        ChannelSwitchResult stale = await owner.SwitchAsync(new(staleId, 99, secondProfile.ProfileId), CancellationToken.None);
        Check(stale.Code == ChannelSwitchCode.StaleGeneration && stale.Generation == 1 && factory.Created.Count == 1, "stale generation created a child");

        factory.FailNextReadiness(secondProfile.ProfileId); Guid failedId = Guid.Parse("10000000-0000-0000-0000-000000000003"); Stopwatch failureTime = Stopwatch.StartNew();
        ChannelSwitchRequest failedRequest = new(failedId, 1, secondProfile.ProfileId); ChannelSwitchResult failed = await owner.SwitchAsync(failedRequest, CancellationToken.None); failureTime.Stop();
        Check(failed.Code == ChannelSwitchCode.ReplacementFailed && failed.Generation == 1 && owner.State?.ProfileId == firstProfile.ProfileId && factory.ActiveCount == 1 && failureTime.Elapsed < TimeSpan.FromSeconds(1), "readiness failure did not roll back promptly and leak-free");
        int failedCreates = factory.Created.Count; Check(await owner.SwitchAsync(failedRequest, CancellationToken.None) == failed && factory.Created.Count == failedCreates, "failed duplicate request created another child");

        LifecycleChild old = factory.Created[0]; Guid appliedId = Guid.Parse("10000000-0000-0000-0000-000000000004"); ChannelSwitchRequest appliedRequest = new(appliedId, 1, secondProfile.ProfileId);
        ChannelSwitchResult applied = await owner.SwitchAsync(appliedRequest, CancellationToken.None);
        Check(applied == new ChannelSwitchResult(appliedId, ChannelSwitchCode.Applied, 2, secondProfile.ProfileId, -1000, 1622) && old.Stopped && factory.ActiveCount == 1, "prepared child was not promoted exactly once with acknowledged defaults");
        int appliedCreates = factory.Created.Count; Check(await owner.SwitchAsync(appliedRequest, CancellationToken.None) == applied && factory.Created.Count == appliedCreates && owner.State?.Generation == 2, "applied duplicate advanced generation or replaced twice");
        Check(!await owner.SetControlAsync(80, 1, -170, 1622, CancellationToken.None) && await owner.SetControlAsync(81, 2, -170, 1622, CancellationToken.None) && owner.State == new ReceiverOwnedState(2, secondProfile.ProfileId, -170, 1622), "live controls crossed their acknowledged generation");

        old.Emit(960); LifecycleChild current = factory.Created.Last(child => !child.ReadinessFails); current.Emit(1_920);
        ReceiverAudioPacket? packet = null; for (int attempt = 0; attempt < 100 && !owner.SessionQueue.TryTake(out packet); attempt++) await Task.Delay(5);
        Check(packet is { Generation: 2, FirstSample: 1_920 } && !owner.SessionQueue.TryTake(out _), "late old-child audio crossed the promoted generation");
        await owner.DisposeAsync(); Check(factory.ActiveCount == 0 && factory.Created.All(child => child.Stopped), "owner disposal retained a child publisher");
    }
    // This is deliberately a process-boundary fixture: two production
    // FleetReceiverAgents run concurrently against an HTTP control-plane
    // harness.  It does not reach a real SDR or server process.
    private static async Task FleetTwoAgentProcessFixtureAsync()
    {
        using FleetAgentProcessHarness harness = new();
        ReceiverChildInputs inputs = new("synthetic", "/synthetic/adapter", "/synthetic/profile", Synthetic: true);
        ApprovedReceiverProfile alpha = new("lab-demo-hill", "371", "Demo Hill", inputs);
        ApprovedReceiverProfile alphaRetuned = new("synthetic-b", "644", "Demo location 62", inputs, PlanId: "shared-air-operations", PlanName: "Demo location 58", PlanGroup: "Other approved categories");
        ApprovedReceiverProfile bravo = new("synthetic-b", "644", "Demo location 62", inputs, PlanId: "shared-air-operations", PlanName: "Demo location 58", PlanGroup: "Other approved categories");
        const string bravoReceiverId = "receiver-bravo";
        LifecycleFactory alphaFactory = new(), bravoFactory = new();
        using HttpClient alphaHttp = new(harness) { BaseAddress = new Uri("https://fleet.test") }, bravoHttp = new(harness) { BaseAddress = new Uri("https://fleet.test") };
        TimeSpan fixtureHeartbeat = TimeSpan.FromMilliseconds(25);
        await using FleetReceiverAgent alphaAgent = new(new ReceiverProfileRegistry([alpha, alphaRetuned], alpha.ProfileId), alphaFactory, alphaHttp, alpha.ProfileId, "synthetic-agent-token", fixtureHeartbeat);
        await using FleetReceiverAgent bravoAgent = new(new ReceiverProfileRegistry([bravo], bravo.ProfileId), bravoFactory, bravoHttp, bravoReceiverId, "synthetic-agent-token", fixtureHeartbeat);
        using CancellationTokenSource stop = new(TimeSpan.FromSeconds(20));
        Task alphaRun = alphaAgent.RunAsync(stop.Token), bravoRun = bravoAgent.RunAsync(stop.Token);
        try
        {
            await harness.WaitForAsync(() => harness.Online(alpha.ProfileId) && harness.Online(bravoReceiverId), "both receiver agents did not complete concurrent Hello/snapshot");
            harness.AssertIsolated(bravoReceiverId, "initial concurrent connection");
            FleetSnapshot desired = harness.Registry.SaveDesired(FleetRole.Admin, alpha.ProfileId, alphaRetuned.ProfileId);
            FleetSession alphaSession = harness.Session(alpha.ProfileId);
            harness.Admit(new(Guid.NewGuid(), alpha.ProfileId, alphaSession.Epoch, alphaSession.BootId, 1, FleetCommandAction.ApplyConfiguration, alphaRetuned.ProfileId, desired.DesiredRevision));
            await harness.WaitForAsync(() => harness.Projection(alpha.ProfileId).AppliedRevision == desired.DesiredRevision && harness.Projection(alpha.ProfileId).State == ReceiverOperatingState.Stopped, "stopped Apply was not acknowledged");
            harness.AssertIsolated(bravoReceiverId, "configure while alpha stopped");

            alphaSession = harness.Session(alpha.ProfileId);
            harness.Admit(new(Guid.NewGuid(), alpha.ProfileId, alphaSession.Epoch, alphaSession.BootId, harness.Projection(alpha.ProfileId).StateVersion, FleetCommandAction.StartReceiver, alphaRetuned.ProfileId, desired.DesiredRevision));
            await harness.WaitForAsync(() => harness.Projection(alpha.ProfileId).State == ReceiverOperatingState.Receiving, "alpha did not start after exact Apply acknowledgement");
            string generation = harness.Projection(alpha.ProfileId).AudioGeneration!;
            FleetAudioListener listener = harness.Audio.Bind(alpha.ProfileId, harness.Session(alpha.ProfileId).Epoch, generation);
            alphaFactory.Created.Last().Emit(960);
            await harness.WaitForAsync(() => listener.TryTake(out _), "current alpha audio generation did not publish");
            Check(alphaFactory.ActiveCount == 1 && alphaFactory.MaximumActiveCount == 1, "fleet Start created more than one child");
            harness.AssertIsolated(bravoReceiverId, "alpha start/audio publication");

            alphaSession = harness.Session(alpha.ProfileId);
            harness.Admit(new(Guid.NewGuid(), alpha.ProfileId, alphaSession.Epoch, alphaSession.BootId, harness.Projection(alpha.ProfileId).StateVersion, FleetCommandAction.SetChannel, alpha.ProfileId));
            await harness.WaitForAsync(() => harness.Projection(alpha.ProfileId).AppliedProfileId == alpha.ProfileId && listener.Retired, "receiving channel replacement did not retire the old generation");
            Check(alphaFactory.ActiveCount == 1 && alphaFactory.MaximumActiveCount == 1, "receiving channel replacement overlapped GNU Radio children");
            alphaSession = harness.Session(alpha.ProfileId);
            harness.Admit(new(Guid.NewGuid(), alpha.ProfileId, alphaSession.Epoch, alphaSession.BootId, harness.Projection(alpha.ProfileId).StateVersion, FleetCommandAction.SetChannel, alphaRetuned.ProfileId));
            await harness.WaitForAsync(() => harness.Projection(alpha.ProfileId).AppliedProfileId == alphaRetuned.ProfileId, "receiving channel restore did not complete");
            Check(alphaFactory.ActiveCount == 1 && alphaFactory.MaximumActiveCount == 1, "receiving channel restore overlapped GNU Radio children");

            alphaSession = harness.Session(alpha.ProfileId);
            harness.Admit(new(Guid.NewGuid(), alpha.ProfileId, alphaSession.Epoch, alphaSession.BootId, harness.Projection(alpha.ProfileId).StateVersion, FleetCommandAction.StopReceiver));
            await harness.WaitForAsync(() => harness.Projection(alpha.ProfileId).State == ReceiverOperatingState.Stopped && listener.Retired, "Stop while listening did not retire alpha generation");
            harness.AssertIsolated(bravoReceiverId, "alpha stop while listening");

            alphaSession = harness.Session(alpha.ProfileId);
            harness.Admit(new(Guid.NewGuid(), alpha.ProfileId, alphaSession.Epoch, alphaSession.BootId, harness.Projection(alpha.ProfileId).StateVersion, FleetCommandAction.StartReceiver, alphaRetuned.ProfileId, desired.DesiredRevision));
            await harness.WaitForAsync(() => harness.Projection(alpha.ProfileId).State == ReceiverOperatingState.Receiving, "alpha restart for child-exit test failed");
            alphaFactory.Created.Last().Exit();
            await harness.WaitForAsync(() => harness.Projection(alpha.ProfileId).State == ReceiverOperatingState.Receiving, "active child exit did not respawn alpha while DesiredRunning", TimeSpan.FromSeconds(12));
            harness.AssertIsolated(bravoReceiverId, "alpha active-child fault");

            // An owned child that cannot prove Stop completion must fail closed
            // after the bounded three-second escalation, without touching Bravo.
            LifecycleFactory orphanFactory = new(); orphanFactory.FailStop(alphaRetuned.ProfileId);
            using HttpClient orphanHttp = new(harness) { BaseAddress = new Uri("https://fleet.test") };
            await using FleetReceiverAgent orphanAgent = new(new ReceiverProfileRegistry([alphaRetuned], alphaRetuned.ProfileId), orphanFactory, orphanHttp, "receiver-orphan", "synthetic-agent-token", fixtureHeartbeat);
            harness.AddReceiver("receiver-orphan", "Orphan", alphaRetuned.ProfileId);
            Task orphanRun = orphanAgent.RunAsync(stop.Token);
            await harness.WaitForAsync(() => harness.Online("receiver-orphan"), "orphan fixture agent did not connect");
            FleetSession orphanSession = harness.Session("receiver-orphan");
            harness.Admit(new(Guid.NewGuid(), "receiver-orphan", orphanSession.Epoch, orphanSession.BootId, 1, FleetCommandAction.StartReceiver, alphaRetuned.ProfileId, 0));
            await harness.WaitForAsync(() => harness.Projection("receiver-orphan").State == ReceiverOperatingState.Receiving, "orphan fixture child did not start");
            orphanSession = harness.Session("receiver-orphan"); Stopwatch escalation = Stopwatch.StartNew();
            harness.Admit(new(Guid.NewGuid(), "receiver-orphan", orphanSession.Epoch, orphanSession.BootId, harness.Projection("receiver-orphan").StateVersion, FleetCommandAction.StopReceiver));
            await harness.WaitForAsync(() => harness.Projection("receiver-orphan").FaultCode == "orphan-suspected", "bounded Stop escalation did not fail closed as orphan-suspected", TimeSpan.FromSeconds(12)); escalation.Stop();
            Check(escalation.Elapsed >= TimeSpan.FromSeconds(2) && escalation.Elapsed < TimeSpan.FromSeconds(8), "orphan Stop escalation was not bounded");
            harness.AssertIsolated(bravoReceiverId, "orphan-suspected cleanup");

            harness.RejectNextHeartbeat(alpha.ProfileId);
            long reconnectEpoch = harness.Session(alpha.ProfileId).Epoch;
            await harness.WaitForAsync(() => harness.Session(alpha.ProfileId).Epoch > reconnectEpoch, "alpha did not reconnect after an unknown-session heartbeat", TimeSpan.FromSeconds(12));
            FleetSession staleAlpha = harness.Session(alpha.ProfileId); harness.Replace(alpha.ProfileId);
            Check(!harness.Registry.ReceiveHeartbeat(staleAlpha), "duplicate-session replacement accepted the old alpha heartbeat");
            await alphaRun.WaitAsync(TimeSpan.FromSeconds(8));
            Check(alphaFactory.ActiveCount == 0 && alphaAgent.State == FleetAgentState.Faulted, "replaced fleet agent did not terminate its RF ownership");
            harness.AssertIsolated(bravoReceiverId, "alpha reconnect and duplicate-session replacement");
            FleetSession bravoSession = harness.Session(bravoReceiverId);
            Check(!harness.Registry.ReceiveHeartbeat(new(alpha.ProfileId, bravoSession.BootId, bravoSession.ConnectionId, bravoSession.Epoch)) && !harness.Registry.ReceiveHeartbeat(staleAlpha), "stale or cross-receiver frames did not fail closed");
            stop.Cancel(); await SuppressCanceledAsync(orphanRun);
        }
        finally
        {
            stop.Cancel(); await SuppressCanceledAsync(alphaRun); await SuppressCanceledAsync(bravoRun);
        }
    }
    private static async Task SuppressCanceledAsync(Task task) { try { await task; } catch (OperationCanceledException) { } }
    private static async Task VerifyAdapterAsync(string adapter, string profile, string mode, bool diagnostics, bool firstExpected, bool? secondExpected, short retained)
    {
        GnuRadioProcessAdapter child = new("python3", adapter, profile, synthetic: true, syntheticControlMode: mode, syntheticDiagnostics: diagnostics);
        using CancellationTokenSource ready = new(TimeSpan.FromSeconds(2)); await child.ReadyAsync(ready.Token);
        V2Receiver receiver = new(child); int processId = receiver.ChildProcessId; using CancellationTokenSource drainStop = new(); Task drain = receiver.DrainPcmAsync(drainStop.Token);
        try
        {
            bool first = await receiver.SetThresholdAsync(1, -180, TimeSpan.FromMilliseconds(500), CancellationToken.None);
            Check(first == firstExpected && receiver.ChildProcessId == processId, $"{mode} control changed child identity or result");
            if (secondExpected is { } expected)
            {
                bool second = await receiver.SetThresholdAsync(2, -190, TimeSpan.FromMilliseconds(500), CancellationToken.None);
                Check(second == expected && receiver.ChildProcessId == processId, $"{mode} follow-up was not bounded");
            }
            Check(receiver.Threshold == retained, $"{mode} changed the last acknowledged threshold");
        }
        finally
        {
            drainStop.Cancel(); await child.StopAsync(CancellationToken.None); try { await drain; } catch (OperationCanceledException) { }
        }
    }
    private static async Task LocalSessionLifecycle()
    {
        string runtime = Path.Combine(Path.GetTempPath(), "t28-v2-test-" + Guid.NewGuid().ToString("N")); string socketPath = Path.Combine(runtime, "receiver.v2.sock"); ulong seen = 0;
        await using (LocalReceiverSession session = new(socketPath, (_, index, _) => { seen = index; return Task.CompletedTask; }))
        {
            session.Start(); Task server = session.RunOneAsync(CancellationToken.None); using Socket client = new(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified); await client.ConnectAsync(new UnixDomainSocketEndPoint(socketPath)); await using NetworkStream stream = new(client, ownsSocket: false);
            await V2Protocol.WriteLocalAsync(stream, LocalType.Hello, LocalBodies.Hello(), CancellationToken.None); await V2Protocol.WriteLocalAsync(stream, LocalType.ChannelRegistry, LocalBodies.ChannelRegistry(new ApprovedChannelProjection[] { new("lab-demo-hill", "371", "Demo Hill") }), CancellationToken.None); await V2Protocol.WriteLocalAsync(stream, LocalType.ReceiverState, LocalBodies.ReceiverState(7, -1000, 1622, "lab-demo-hill"), CancellationToken.None); await V2Protocol.WriteLocalAsync(stream, LocalType.Audio, LocalBodies.Audio(7, 960, new byte[V2Protocol.PcmBytes]), CancellationToken.None);
            client.Shutdown(SocketShutdown.Both); try { await server; } catch (EndOfStreamException) { } Check(seen == 960 && session.EffectiveState == null, "local vertical slice failed or stale state remained");
        }
        Check(!File.Exists(socketPath) && !Directory.Exists(runtime), "v2 runtime cleanup retained socket or directory");
    }
    private static async Task LocalSessionRetirementRacesAsync()
    {
        string runtime = Path.Combine(Path.GetTempPath(), "t28-v2-race-" + Guid.NewGuid().ToString("N")); string socketPath = Path.Combine(runtime, "receiver.v2.sock");
        await using LocalReceiverSession session = new(socketPath, (_, _, pcm) => { Check(pcm.Length == V2Protocol.PcmBytes, "local session corrupted PCM"); return Task.CompletedTask; });
        session.Start();
        Task firstServer = session.RunOneAsync(CancellationToken.None); await using LocalClient first = await LocalClient.ConnectAsync(socketPath);
        Task<bool> acknowledged = SetWhenAvailableAsync(session, 1, -170); Check(await acknowledged, "current receiver did not acknowledge squelch");

        Task secondServer = session.RunOneAsync(CancellationToken.None); await using LocalClient replacement = await LocalClient.ConnectAsync(socketPath, acknowledge: false);
        Task<bool> duringRetirement = SetWhenAvailableAsync(session, 2, -180);
        await replacement.WaitForCommandAsync();
        await replacement.DisposeAsync();
        Check(!await duringRetirement.WaitAsync(TimeSpan.FromSeconds(1)), "retired receiver control was acknowledged");
        await SuppressEndAsync(firstServer); await SuppressEndAsync(secondServer);
        Check(!await session.SetSquelchAsync(3, -170, CancellationToken.None), "disconnected receiver accepted a stale control");

        Task finalServer = session.RunOneAsync(CancellationToken.None); await using LocalClient final = await LocalClient.ConnectAsync(socketPath);
        Check(await SetWhenAvailableAsync(session, 4, -170), "replacement receiver did not acknowledge exact control");
        await final.DisposeAsync(); await SuppressEndAsync(finalServer);
    }
    private static async Task<bool> SetWhenAvailableAsync(LocalReceiverSession session, ulong id, short value)
    {
        for (int attempt = 0; attempt < 100; attempt++)
        {
            if (await session.SetSquelchAsync(id + (ulong)attempt * 100, value, CancellationToken.None)) return true;
            await Task.Delay(5);
        }
        return false;
    }
    private static async Task SuppressEndAsync(Task task)
    {
        try { await task; } catch (EndOfStreamException) { } catch (ObjectDisposedException) { }
    }
    private static async Task PhysicalRatePacketOwnershipAsync()
    {
        // The deterministic source writes 26 fixed packets every 520 ms, like
        // the observed stdout bursts.  No PCM payload is inspected: only
        // packet metadata, timing, and source indices cross this test.
        FakeChild child = new(); V2Receiver receiver = new(child); BrowserListener browser = new(); browser.BindGeneration(1);
        Task controls = Task.Run(async () =>
        {
            for (ulong id = 1; id <= 120; id++) Check(await receiver.SetThresholdAsync(id, -170, TimeSpan.FromMilliseconds(100), CancellationToken.None), "concurrent squelch control failed");
        });
        // A focused 10 s cadence sample is sufficient for this change's
        // lifecycle boundary; longer aggregate soak belongs to its own gate.
        const int packetCount = 508, burstPackets = 26;
        List<(ulong Index, TimeSpan At)> delivered = []; Stopwatch elapsed = Stopwatch.StartNew(); PcmMediaPacer pacer = new();
        Check(browser.Control(new ClientMessage(1, false, null, 0)) == BrowserControlOutcome.Accepted && browser.Control(new ClientMessage(1, null, null, 4)) == BrowserControlOutcome.Accepted, "paced flow/credit rejected");
        Task pace = pacer.PaceAsync((index, pcm) =>
        {
            browser.Publish(1, index, pcm); Check(browser.TrySend(out byte[]? wire) && wire is not null, "paced frame overflowed the fixed browser queue");
            Check(BinaryPrimitives.ReadUInt64BigEndian(wire.AsSpan(8)) == 1, "paced frame lost its owning attempt"); ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(wire.AsSpan(16)); ulong sentIndex = BinaryPrimitives.ReadUInt64BigEndian(wire.AsSpan(24));
            browser.Control(new ClientMessage(1, null, sequence, 1, true)); delivered.Add((sentIndex, elapsed.Elapsed));
        }, CancellationToken.None);
        for (int packet = 0; packet < packetCount; packet++)
        {
            await pacer.EnqueueAsync((ulong)packet * V2Protocol.SamplesPerPacket, new byte[V2Protocol.PcmBytes], CancellationToken.None);
            if ((packet + 1) % burstPackets == 0 && packet + 1 < packetCount) await Task.Delay(520);
        }
        pacer.Complete(); await pace; await controls;
        Check(delivered.Count == packetCount && delivered.Select(item => item.Index).SequenceEqual(Enumerable.Range(0, packetCount).Select(item => (ulong)item * V2Protocol.SamplesPerPacket)), "pacer lost, duplicated, or reordered source indices");
        double[] gaps = delivered.Zip(delivered.Skip(1), (left, right) => (right.At - left.At).TotalMilliseconds).Order().ToArray();
        Check(gaps[gaps.Length / 2] is > 15 and < 25 && gaps[(int)Math.Ceiling(gaps.Length * .95) - 1] <= 30 && gaps[^1] <= 100, "bursty source did not remain at media cadence");
        Check(receiver.ChildProcessId == 42 && receiver.Threshold == -170, "packet/control test changed receiver identity");
    }
    private sealed class HangControlHandler(bool hangHeartbeat) : HttpMessageHandler
    {
        public int Heartbeats { get; private set; }
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            string path = request.RequestUri!.AbsolutePath;
            if (path == "/v2/agent/hello")
            {
                T28.V2.Receiver.FleetHello hello = await request.Content!.ReadFromJsonAsync<T28.V2.Receiver.FleetHello>(cancellationToken: token) ?? throw new InvalidDataException();
                return new(System.Net.HttpStatusCode.OK) { Content = JsonContent.Create(new T28.V2.Receiver.FleetHelloAccepted(hello.ReceiverId, hello.BootId, 1, 5)) };
            }
            if (path == "/v2/agent/heartbeat")
            {
                if (hangHeartbeat) await Task.Delay(Timeout.InfiniteTimeSpan, token);
                Heartbeats++;
                return new(System.Net.HttpStatusCode.NoContent);
            }
            if (path is "/v2/agent/snapshot" or "/v2/agent/acknowledgement") return new(System.Net.HttpStatusCode.NoContent);
            if (path == "/v2/agent/commands") return new(System.Net.HttpStatusCode.OK) { Content = JsonContent.Create(Array.Empty<T28.V2.Receiver.FleetAgentCommand>()) };
            return new(System.Net.HttpStatusCode.NotFound);
        }
    }
    private sealed class FleetAgentProcessHarness : HttpMessageHandler, IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "t28-v2-two-agent-" + Guid.NewGuid().ToString("N"));
        private readonly Dictionary<string, FleetSession> _sessions = []; private readonly Dictionary<string, (FleetSession Session, FleetSafeProjection Projection, int Listeners)> _bravoBaseline = [];
        private string? _rejectHeartbeat;
        public string? LastError { get; private set; }
        public FleetRegistry Registry { get; } public FleetAudioHub Audio { get; } = new();
        public FleetAgentProcessHarness()
        {
            JsonFleetStore store = new(Path.Combine(_directory, "fleet.json"));
            store.Save([
                new("lab-demo-hill", "Alpha", true, 0, "lab-demo-hill", 0, FleetSafeProjection.Stopped("lab-demo-hill"), null, [], DesiredRunning: false),
                new("receiver-bravo", "Bravo", true, 0, "receiver-bravo", 0, FleetSafeProjection.Stopped("receiver-bravo"), null, [], DesiredRunning: false),
                new("receiver-orphan", "Orphan", true, 0, "synthetic-b", 0, FleetSafeProjection.Stopped("synthetic-b"), null, [], DesiredRunning: false)]);
            Registry = new FleetRegistry(store);
        }
        public bool Online(string receiverId) { FleetSnapshot snapshot = Registry.Snapshot(receiverId); return snapshot.Connectivity == FleetConnectivity.Online && snapshot.ProjectionCurrent; }
        public FleetSession Session(string receiverId) => _sessions[receiverId];
        public FleetSafeProjection Projection(string receiverId) => Registry.Snapshot(receiverId).Projection;
        public void AddReceiver(string receiverId, string display, string profile) { Check(receiverId == "receiver-orphan" && display == "Orphan" && profile == "synthetic-b", "fixture attempted an unregistered receiver"); }
        public void Admit(FleetCommand command) => Check(Registry.Admit(FleetRole.Admin, command).Outcome == FleetCommandOutcome.Accepted, "fixture command was not admitted");
        public void Replace(string receiverId) { FleetSession prior = Session(receiverId); _sessions[receiverId] = Registry.AcceptHello(receiverId, prior.BootId + "x", "replacement-connection"); }
        public void RejectNextHeartbeat(string receiverId) => _rejectHeartbeat = receiverId;
        public async Task WaitForAsync(Func<bool> predicate, string failure, TimeSpan? timeout = null)
        {
            Stopwatch elapsed = Stopwatch.StartNew(); TimeSpan bound = timeout ?? TimeSpan.FromSeconds(8);
            while (elapsed.Elapsed < bound) { if (predicate()) return; await Task.Delay(20); }
            throw new InvalidOperationException(failure + ": " + LastError);
        }
        public void AssertIsolated(string receiverId, string phase)
        {
            FleetSession session = Session(receiverId); FleetSafeProjection projection = Projection(receiverId); int listeners = Audio.ListenerCount(receiverId);
            if (!_bravoBaseline.TryGetValue(receiverId, out var baseline)) { _bravoBaseline[receiverId] = (session, projection, listeners); return; }
            bool projectionEqual = baseline.Projection with { Channels = null } == projection with { Channels = null } && (baseline.Projection.Channels ?? []).SequenceEqual(projection.Channels ?? []);
            Check(baseline.Session == session && projectionEqual && baseline.Listeners == listeners && Registry.Pending(session).Count == 0, "receiver isolation changed " + receiverId + " during " + phase);
        }
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            string path = request.RequestUri!.AbsolutePath;
            try
            {
                if (path == "/v2/agent/hello")
                {
                    T28.V2.Receiver.FleetHello hello = await request.Content!.ReadFromJsonAsync<T28.V2.Receiver.FleetHello>(cancellationToken: token) ?? throw new InvalidDataException();
                    FleetSession session = Registry.AcceptHello(hello.ReceiverId, hello.BootId, hello.ConnectionId); _sessions[session.ReceiverId] = session;
                    return Json(new T28.V2.Receiver.FleetHelloAccepted(session.ReceiverId, session.BootId, session.Epoch, 5));
                }
                if (path == "/v2/agent/heartbeat")
                {
                    T28.V2.Receiver.FleetEnvelope envelope = await request.Content!.ReadFromJsonAsync<T28.V2.Receiver.FleetEnvelope>(cancellationToken: token) ?? throw new InvalidDataException();
                    FleetSession session = new(envelope.ReceiverId, envelope.BootId, envelope.ConnectionId, envelope.SessionEpoch);
                    if (_rejectHeartbeat == envelope.ReceiverId) { _rejectHeartbeat = null; return Conflict("unknown"); }
                    return Registry.ReceiveHeartbeat(session) ? Status(System.Net.HttpStatusCode.NoContent) : Conflict(Registry.SessionStatus(session) == FleetSessionStatus.Replaced ? "replaced" : "unknown");
                }
                if (path == "/v2/agent/snapshot")
                {
                    T28.V2.Receiver.FleetSnapshotEnvelope message = await request.Content!.ReadFromJsonAsync<T28.V2.Receiver.FleetSnapshotEnvelope>(cancellationToken: token) ?? throw new InvalidDataException(); FleetSession session = new(message.ReceiverId, message.BootId, message.ConnectionId, message.SessionEpoch);
                    if (!Registry.ReceiveSnapshot(session, new((ReceiverOperatingState)message.Projection.State, message.Projection.StateVersion, message.Projection.AudioGeneration, message.Projection.AppliedProfileId, message.Projection.AppliedRevision, message.Projection.Hardware, message.Projection.FaultCode, message.Projection.ThresholdTenths, message.Projection.CtcssTenths, message.Projection.Channels))) return Conflict(Registry.SessionStatus(session) == FleetSessionStatus.Replaced ? "replaced" : "unknown");
                    Registry.ReconcileRunningIntent(session); Audio.Promote(session.ReceiverId, session.Epoch, message.Projection.AudioGeneration); return Status(System.Net.HttpStatusCode.NoContent);
                }
                if (path == "/v2/agent/commands")
                {
                    Dictionary<string, string> query = request.RequestUri.Query.TrimStart('?').Split('&').Select(value => value.Split('=', 2)).ToDictionary(value => value[0], value => Uri.UnescapeDataString(value[1]), StringComparer.Ordinal); FleetSession session = new(query["receiverId"], query["bootId"], query["connectionId"], long.Parse(query["sessionEpoch"], System.Globalization.CultureInfo.InvariantCulture)); if (Registry.SessionStatus(session) != FleetSessionStatus.Current) return Conflict(Registry.SessionStatus(session) == FleetSessionStatus.Replaced ? "replaced" : "unknown");
                    T28.V2.Receiver.FleetAgentCommand[] commands = Registry.Pending(session).Select(command => new T28.V2.Receiver.FleetAgentCommand(command.RequestId, command.ReceiverId, command.SessionEpoch, command.BootId, command.ExpectedStateVersion, (FleetAgentAction)command.Action, command.ProfileId, command.DesiredRevision)).ToArray();
                    return Json(commands);
                }
                if (path == "/v2/agent/acknowledgement")
                {
                    T28.V2.Receiver.FleetAgentAcknowledgement message = await request.Content!.ReadFromJsonAsync<T28.V2.Receiver.FleetAgentAcknowledgement>(cancellationToken: token) ?? throw new InvalidDataException();
                    FleetProjection projection = message.Projection; FleetSession session = new(message.ReceiverId, message.BootId, message.ConnectionId, message.SessionEpoch); bool complete = Registry.Complete(session, message.RequestId, (FleetCommandOutcome)message.Outcome, new((ReceiverOperatingState)projection.State, projection.StateVersion, projection.AudioGeneration, projection.AppliedProfileId, projection.AppliedRevision, projection.Hardware, projection.FaultCode, projection.ThresholdTenths, projection.CtcssTenths, projection.Channels));
                    if (complete) Audio.Promote(session.ReceiverId, session.Epoch, projection.AudioGeneration);
                    return complete ? Status(System.Net.HttpStatusCode.NoContent) : Conflict(Registry.SessionStatus(session) == FleetSessionStatus.Replaced ? "replaced" : "unknown");
                }
                if (path == "/v2/agent/audio")
                {
                    T28.V2.Receiver.FleetAudioEnvelope message = await request.Content!.ReadFromJsonAsync<T28.V2.Receiver.FleetAudioEnvelope>(cancellationToken: token) ?? throw new InvalidDataException(); FleetSession session = new(message.ReceiverId, message.BootId, message.ConnectionId, message.SessionEpoch);
                    if (!Registry.AcceptAudio(session, message.AudioGeneration)) return Conflict(Registry.SessionStatus(session) == FleetSessionStatus.Replaced ? "replaced" : "unknown");
                    Audio.Publish(session.ReceiverId, session.Epoch, message.AudioGeneration, message.FirstSample, message.Pcm); return Status(System.Net.HttpStatusCode.NoContent);
                }
                return Status(System.Net.HttpStatusCode.NotFound);
            }
            catch (Exception error) { LastError = error.GetType().Name + ": " + error.Message; return Status(System.Net.HttpStatusCode.BadRequest); }
        }
        private static HttpResponseMessage Json<T>(T value) => new(System.Net.HttpStatusCode.OK) { Content = JsonContent.Create(value) };
        private static HttpResponseMessage Status(System.Net.HttpStatusCode code) => new(code);
        private static HttpResponseMessage Conflict(string fence) { HttpResponseMessage response = Status(System.Net.HttpStatusCode.Conflict); response.Headers.Add("X-T28-V2-Session-Fence", fence); return response; }
        public new void Dispose() { base.Dispose(); if (Directory.Exists(_directory)) Directory.Delete(_directory, true); }
    }
    private sealed class FragmentSocket : WebSocket
    {
        private readonly Queue<(WebSocketMessageType Type, byte[] Bytes, bool End)> _incoming = [];
        private readonly SemaphoreSlim _available = new(0);
        private WebSocketState _state = WebSocketState.Open;
        private WebSocketCloseStatus? _closeStatus;
        private string? _closeStatusDescription;
        public Action? BeforeReceive { get; set; }
        public Action<ulong>? BinarySent { get; set; }
        public List<ulong> BinarySequences { get; } = [];
        public override WebSocketCloseStatus? CloseStatus => _closeStatus;
        public override string? CloseStatusDescription => _closeStatusDescription;
        public override WebSocketState State => _state;
        public override string? SubProtocol => V2Protocol.Subprotocol;
        public void AddText(string value, bool oneByte = false, bool endOfMessage = true, int splitAt = 0)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            if (splitAt > 0 && splitAt < bytes.Length)
            {
                Enqueue((WebSocketMessageType.Text, bytes[..splitAt], false)); Enqueue((WebSocketMessageType.Text, bytes[splitAt..], endOfMessage)); return;
            }
            AddTextBytes(bytes, endOfMessage, oneByte);
        }
        public void AddTextBytes(byte[] value, bool endOfMessage, bool oneByte = false)
        {
            if (!oneByte) { Enqueue((WebSocketMessageType.Text, value, endOfMessage)); return; }
            for (int index = 0; index < value.Length; index++) Enqueue((WebSocketMessageType.Text, [value[index]], index == value.Length - 1 && endOfMessage));
        }
        public void AddBinary(byte[] value, bool endOfMessage) => Enqueue((WebSocketMessageType.Binary, value, endOfMessage));
        public void AddClose() => Enqueue((WebSocketMessageType.Close, [], true));
        public override void Abort() => _state = WebSocketState.Aborted;
        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) { _closeStatus = closeStatus; _closeStatusDescription = statusDescription; _state = WebSocketState.Closed; return Task.CompletedTask; }
        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) { _closeStatus = closeStatus; _closeStatusDescription = statusDescription; _state = WebSocketState.CloseSent; return Task.CompletedTask; }
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            BeforeReceive?.Invoke();
            return ReceiveQueuedAsync(buffer, cancellationToken);
        }
        private void Enqueue((WebSocketMessageType Type, byte[] Bytes, bool End) item)
        {
            _incoming.Enqueue(item); _available.Release();
        }
        private async Task<WebSocketReceiveResult> ReceiveQueuedAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            await _available.WaitAsync(cancellationToken);
            (WebSocketMessageType type, byte[] bytes, bool end) = _incoming.Dequeue(); Array.Copy(bytes, 0, buffer.Array!, buffer.Offset, bytes.Length); if (type == WebSocketMessageType.Close) _state = WebSocketState.Closed;
            return new WebSocketReceiveResult(bytes.Length, type, end);
        }
        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            if (messageType == WebSocketMessageType.Binary) { ulong sequence = BinaryPrimitives.ReadUInt64BigEndian(buffer.AsSpan(16)); BinarySequences.Add(sequence); BinarySent?.Invoke(sequence); }
            return Task.CompletedTask;
        }
        public override void Dispose() { _state = WebSocketState.Closed; }
    }
    private sealed class LocalClient : IAsyncDisposable
    {
        private readonly Socket _socket; private readonly NetworkStream _stream; private readonly bool _acknowledge; private readonly TaskCompletionSource _command = new(TaskCreationOptions.RunContinuationsAsynchronously); private readonly SemaphoreSlim _write = new(1, 1); private readonly CancellationTokenSource _stop = new(); private readonly Task _read; private int _disposed;
        private LocalClient(Socket socket, NetworkStream stream, bool acknowledge) { _socket = socket; _stream = stream; _acknowledge = acknowledge; _read = ReadAsync(); }
        public static async Task<LocalClient> ConnectAsync(string path, bool acknowledge = true)
        {
            Socket socket = new(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified); await socket.ConnectAsync(new UnixDomainSocketEndPoint(path)); NetworkStream stream = new(socket, ownsSocket: false); await V2Protocol.WriteLocalAsync(stream, LocalType.Hello, LocalBodies.Hello(), CancellationToken.None); await V2Protocol.WriteLocalAsync(stream, LocalType.ChannelRegistry, LocalBodies.ChannelRegistry(new ApprovedChannelProjection[] { new("lab-demo-hill", "371", "Demo Hill") }), CancellationToken.None); await V2Protocol.WriteLocalAsync(stream, LocalType.ReceiverState, LocalBodies.ReceiverState(1, -1000, 1622, "lab-demo-hill"), CancellationToken.None); return new LocalClient(socket, stream, acknowledge);
        }
        public Task WaitForCommandAsync() => _command.Task.WaitAsync(TimeSpan.FromSeconds(1));
        private async Task ReadAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    LocalEnvelope command = await V2Protocol.ReadLocalAsync(_stream, _stop.Token); if (command.Type is not (LocalType.SetSquelch or LocalType.SetControl)) throw new InvalidDataException("unexpected local control");
                    _command.TrySetResult();
                    if (_acknowledge)
                    {
                        await _write.WaitAsync(_stop.Token); try { if (command.Type == LocalType.SetSquelch) { (ulong id, short value) = LocalBodies.Squelch(command.Body); await V2Protocol.WriteLocalAsync(_stream, LocalType.SquelchApplied, LocalBodies.Squelch(id, value), _stop.Token); } else { (ulong id, ulong generation, short value, short tone) = LocalBodies.Control(command.Body); await V2Protocol.WriteLocalAsync(_stream, LocalType.ControlApplied, LocalBodies.Control(id, generation, value, tone), _stop.Token); } await _stream.FlushAsync(_stop.Token); } finally { _write.Release(); }
                    }
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            catch (EndOfStreamException) { }
            catch (ObjectDisposedException) { }
        }
        public async ValueTask DisposeAsync() { if (Interlocked.Exchange(ref _disposed, 1) != 0) return; _stop.Cancel(); _socket.Dispose(); _stream.Dispose(); try { await _read; } catch { } _write.Dispose(); _stop.Dispose(); }
    }
    private sealed class LifecycleFactory : IReceiverChildFactory
    {
        private readonly HashSet<string> _failNext = [], _failStop = [];
        private int _active, _maximumActive;
        public List<LifecycleChild> Created { get; } = [];
        public int ActiveCount => Volatile.Read(ref _active);
        public int MaximumActiveCount => Volatile.Read(ref _maximumActive);
        public void FailNextReadiness(string profileId) => _failNext.Add(profileId);
        public void FailStop(string profileId) => _failStop.Add(profileId);
        public Task<IGnuRadioV2> CreateAsync(ApprovedReceiverProfile profile, CancellationToken token)
        {
            token.ThrowIfCancellationRequested(); bool fail = _failNext.Remove(profile.ProfileId), failStop = _failStop.Remove(profile.ProfileId); int active = Interlocked.Increment(ref _active); int maximum; while ((maximum = Volatile.Read(ref _maximumActive)) < active && Interlocked.CompareExchange(ref _maximumActive, active, maximum) != maximum) { }
            LifecycleChild child = new(profile.ProfileId, fail, failStop, () => Interlocked.Decrement(ref _active)); Created.Add(child); return Task.FromResult<IGnuRadioV2>(child);
        }
    }
    private sealed class LifecycleChild(string profileId, bool readinessFails, bool stopFails, Action stopped) : IGnuRadioV2
    {
        private readonly Channel<(ulong FirstSample, byte[] Pcm)> _pcm = Channel.CreateUnbounded<(ulong, byte[])>();
        private int _stopped;
        public string ProfileId { get; } = profileId;
        public bool ReadinessFails { get; } = readinessFails;
        public bool Stopped => Volatile.Read(ref _stopped) != 0;
        public int ProcessId => ProfileId.GetHashCode(StringComparison.Ordinal);
        public Task ReadyAsync(CancellationToken token) => ReadinessFails ? Task.FromException(new InvalidOperationException("synthetic readiness failure")) : Task.CompletedTask;
        public Task<(short Threshold, short CtcssTenths)> SetControlAsync(ulong requestId, short threshold, short ctcssTenths, CancellationToken token) => Task.FromResult((threshold, ctcssTenths));
        public IAsyncEnumerable<(ulong FirstSample, byte[] Pcm)> ReadPcmAsync(CancellationToken token) => _pcm.Reader.ReadAllAsync(token);
        public void Emit(ulong firstSample) => _pcm.Writer.TryWrite((firstSample, new byte[V2Protocol.PcmBytes]));
        public void Exit() => _pcm.Writer.TryComplete();
        public async Task StopAsync(CancellationToken token) { if (stopFails) await Task.Delay(Timeout.InfiniteTimeSpan, token); if (Interlocked.Exchange(ref _stopped, 1) == 0) { _pcm.Writer.TryComplete(); stopped(); } }
    }
    private sealed class FakeChild : IGnuRadioV2
    {
        public bool Timeout { get; set; } public int ProcessId => 42;
        public Task ReadyAsync(CancellationToken token) => Task.CompletedTask;
        public async Task<(short Threshold, short CtcssTenths)> SetControlAsync(ulong requestId, short threshold, short ctcssTenths, CancellationToken token) { if (requestId == 0) throw new InvalidDataException("request id is required"); await Task.Delay(Timeout ? 50 : 1, token); return (threshold, ctcssTenths); }
        public async IAsyncEnumerable<(ulong FirstSample, byte[] Pcm)> ReadPcmAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token) { await Task.CompletedTask; yield break; }
        public Task StopAsync(CancellationToken token) => Task.CompletedTask;
    }
}
