using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using T28.V2.Contracts;

namespace T28.V2.Server;

public sealed record V2BrowserSecurity(string Origin, string Host, string CookieName, string SessionValue)
{
    public bool MatchesOriginHost(string? origin, string? host) => origin == Origin && host == Host;
    public bool HasSession(string? cookie) => cookie?.Split(';', StringSplitOptions.TrimEntries).Contains($"{CookieName}={SessionValue}") == true;
    public bool Accept(string? origin, string? host, string? cookie, IReadOnlyList<string> protocols) =>
        !string.IsNullOrWhiteSpace(SessionValue) && MatchesOriginHost(origin, host) && HasSession(cookie) &&
        protocols.Count == 1 && protocols[0] == V2Protocol.Subprotocol;
}
public sealed class V2SessionIssuer(string origin, string host, FleetRole role = FleetRole.Admin)
{
    public string Origin { get; } = origin;
    public string Host { get; } = host;
    public string Session { get; } = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
    public string Csrf { get; } = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
    public FleetRole Role { get; } = role;
    public bool CheckCsrf(string? value) => string.Equals(value, Csrf, StringComparison.Ordinal);
}
public sealed class BrowserListener
{
    // Both acknowledgement queues are deliberately tiny.  They retain only
    // exact sequences that crossed this attempt's socket, never source frames
    // that were dropped at the live edge.
    private const int MaxConsecutiveControlAnomalies = 3;
    private readonly LiveQueue<BrowserPacket> _queue = new(); private readonly Queue<ulong> _sent = new(); private readonly Queue<ulong> _retired = new(); private readonly object _gate = new(); private ulong _sequence; private ulong _attempt; private ulong _generation; private int _credit; private int _consecutiveControlAnomalies; private bool _paused = true, _generationRetired;
    public bool Paused { get { lock (_gate) return _paused; } }
    public bool Lagging { get { lock (_gate) return _sent.Count > LiveQueue<BrowserPacket>.Capacity; } }
    public int QueueDepth { get { lock (_gate) return _queue.Count; } }
    public int Credit { get { lock (_gate) return _credit; } }
    public bool GenerationRetired { get { lock (_gate) return _generationRetired; } }
    public void BindGeneration(ulong generation)
    {
        if (generation == 0) throw new ArgumentOutOfRangeException(nameof(generation));
        lock (_gate)
        {
            if (_generation == generation) return;
            if (_generation != 0) _generationRetired = true;
            _generation = generation; ClearAttempt();
        }
    }
    public void RetireGeneration() { lock (_gate) { _generationRetired = true; ClearAttempt(); } }
    public void Publish(ulong generation, ulong firstSample, byte[] pcm) { lock (_gate) if (!_paused && !_generationRetired && generation == _generation) _queue.Push(new BrowserPacket(0, ++_sequence, firstSample, pcm)); }
    public bool TrySend(out byte[]? packet)
    {
        lock (_gate)
        {
            packet = null; if (_paused || _generationRetired || _credit == 0 || _sent.Count > LiveQueue<BrowserPacket>.Capacity || !_queue.TryTake(out BrowserPacket? source) || source is null) return false;
            _credit--; _sent.Enqueue(source.Sequence); packet = V2Protocol.BrowserPacket(_attempt, source.Sequence, source.FirstSample, source.Pcm); return true;
        }
    }
    public BrowserControlOutcome Control(ClientMessage message)
    {
        lock (_gate)
        {
        if (message.Paused is bool paused)
        {
            if (!paused && (_attempt == 0 || message.Attempt > _attempt)) { _attempt = message.Attempt; _paused = false; ClearAttempt(); ResetAnomalies(); return BrowserControlOutcome.Accepted; }
            if (paused && message.Attempt == _attempt) { _paused = true; ClearAttempt(); ResetAnomalies(); return BrowserControlOutcome.Accepted; }
            return BrowserControlOutcome.StaleAttempt;
        }
        if (_paused || message.Attempt != _attempt) return BrowserControlOutcome.StaleAttempt;
        if (message.IsConsume)
        {
            if (message.Played is not ulong consumed) return Anomaly(BrowserControlOutcome.IgnoredInvalidPlayedSequence, BrowserControlOutcome.InvalidPlayedSequence);
            BrowserControlOutcome acknowledgement = RetirePlayed(consumed);
            // A duplicated worklet callback is a harmless acknowledgement of
            // already-retired media, not a second replacement-packet grant.
            if (acknowledgement == BrowserControlOutcome.Accepted) return GrantReplacement();
            if (acknowledgement == BrowserControlOutcome.StaleAttempt) { ResetAnomalies(); return BrowserControlOutcome.Accepted; }
            return Anomaly(BrowserControlOutcome.IgnoredInvalidPlayedSequence, BrowserControlOutcome.InvalidPlayedSequence);
        }
        if (message.Credit > 0)
        {
            // Only an initial/re-prime four-packet grant is standalone. Each
            // runtime replacement is coupled to its exact consume acknowledgement.
            if (message.Credit != 4 || _credit != 0 || _sent.Count != 0) return Anomaly(BrowserControlOutcome.IgnoredExcessiveCredit, BrowserControlOutcome.ExcessiveCredit);
            if (_credit + message.Credit > 4 || _sent.Count + _credit + message.Credit > LiveQueue<BrowserPacket>.Capacity) return Anomaly(BrowserControlOutcome.IgnoredExcessiveCredit, BrowserControlOutcome.ExcessiveCredit);
            _credit += message.Credit; ResetAnomalies(); return BrowserControlOutcome.Accepted;
        }
        // Published source frames may accumulate at the bounded live edge while
        // the worklet consumes its startup buffer.  Only a packet actually sent
        // to this attempt can retire browser credit; source publication is not
        // browser in-flight media.
        if (message.Played is not ulong played) return Anomaly(BrowserControlOutcome.IgnoredInvalidPlayedSequence, BrowserControlOutcome.InvalidPlayedSequence);
        BrowserControlOutcome playedOutcome = RetirePlayed(played);
        if (playedOutcome is BrowserControlOutcome.Accepted or BrowserControlOutcome.StaleAttempt) { ResetAnomalies(); return BrowserControlOutcome.Accepted; }
        return Anomaly(BrowserControlOutcome.IgnoredInvalidPlayedSequence, BrowserControlOutcome.InvalidPlayedSequence);
        }
    }
    private BrowserControlOutcome RetirePlayed(ulong played)
    {
        if (_retired.Contains(played)) return BrowserControlOutcome.StaleAttempt;
        if (_sent.Contains(played))
        {
            while (_sent.TryDequeue(out ulong retired))
            {
                RememberRetired(retired);
                if (retired == played) break;
            }
            return BrowserControlOutcome.Accepted;
        }
        return BrowserControlOutcome.IgnoredInvalidPlayedSequence;
    }
    private BrowserControlOutcome GrantReplacement()
    {
        if (_credit + 1 > 4 || _sent.Count + _credit + 1 > LiveQueue<BrowserPacket>.Capacity) return Anomaly(BrowserControlOutcome.IgnoredExcessiveCredit, BrowserControlOutcome.ExcessiveCredit);
        _credit++; ResetAnomalies(); return BrowserControlOutcome.Accepted;
    }
    private void ClearAttempt() { _queue.Clear(); _credit = 0; _sent.Clear(); _retired.Clear(); }
    private void RememberRetired(ulong sequence) { if (_retired.Count == LiveQueue<BrowserPacket>.Capacity) _retired.Dequeue(); _retired.Enqueue(sequence); }
    private void ResetAnomalies() => _consecutiveControlAnomalies = 0;
    private BrowserControlOutcome Anomaly(BrowserControlOutcome ignored, BrowserControlOutcome exhausted) => ++_consecutiveControlAnomalies <= MaxConsecutiveControlAnomalies ? ignored : exhausted;
}
/// <summary>Sanitized control outcomes.  No payload, session, or media content is retained.</summary>
public enum BrowserControlOutcome { Accepted, StaleAttempt, IgnoredInvalidPlayedSequence, InvalidPlayedSequence, IgnoredExcessiveCredit, ExcessiveCredit }
public sealed record ReceiverControlState(ulong Generation, short ThresholdTenths, short CtcssTenths, string ProfileId);
public sealed record ChannelProjectionState(Guid? RequestId, string Status, string? RequestedProfileId, string? Reason);
public sealed record ReceiverSessionSnapshot(ReceiverControlState? State, IReadOnlyList<ApprovedChannelProjection> Channels, ChannelProjectionState Channel);
/// <summary>Server-owned local UDS. It never buffers PCM outside each listener's fixed live queue.</summary>
public sealed class LocalReceiverSession : IAsyncDisposable
{
    private readonly string _path; private readonly Func<ulong, ulong, byte[], Task> _audio; private readonly Action<ReceiverControlState?> _stateChanged; private Socket? _listener; private NetworkStream? _stream; private ReceiverControlState? _state; private IReadOnlyList<ApprovedChannelProjection> _channels = []; private ChannelProjectionState _channel = new(null, "unavailable", null, null); private readonly SemaphoreSlim _write = new(1, 1); private readonly Dictionary<ulong, TaskCompletionSource<(ulong, short, short)>> _controlPending = []; private readonly Dictionary<Guid, PendingChannel> _channelPending = []; private readonly Dictionary<Guid, ChannelSwitchResult> _channelHistory = []; private readonly object _gate = new();
    public LocalReceiverSession(string path, Func<ulong, ulong, byte[], Task> audio, Action<ReceiverControlState?>? stateChanged = null) { _path = path; _audio = audio; _stateChanged = stateChanged ?? (_ => { }); }
    public string Path => _path;
    public ReceiverControlState? EffectiveState { get { lock (_gate) return _state; } }
    public ReceiverSessionSnapshot Snapshot { get { lock (_gate) return new(_state, _channels.ToArray(), _channel); } }
    public void ProjectChannelRejection(Guid requestId, string reason) { lock (_gate) if (_state is not null) _channel = new(requestId, "rejected", null, reason); }
    public bool TryReplayChannel(Guid requestId, out ChannelSwitchResult? result) { lock (_gate) return _channelHistory.TryGetValue(requestId, out result); }
    public bool HasPendingChannel(Guid requestId) { lock (_gate) return _channelPending.ContainsKey(requestId); }
    public void Start()
    {
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(_path)!); if (File.Exists(_path)) File.Delete(_path);
        if (!OperatingSystem.IsLinux()) throw new PlatformNotSupportedException("T28 v2 local receiver sessions require Unix-domain sockets.");
        _listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified); _listener.Bind(new UnixDomainSocketEndPoint(_path)); _listener.Listen(1); File.SetUnixFileMode(_path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
    }
    public async Task RunOneAsync(CancellationToken token)
    {
        if (_listener is null) throw new InvalidOperationException("v2 local session is not started.");
        using Socket socket = await _listener.AcceptAsync(token); await using NetworkStream stream = new(socket, ownsSocket: false);
        try
        {
            LocalEnvelope hello = await V2Protocol.ReadLocalAsync(stream, token); if (hello.Type != LocalType.Hello) throw new InvalidDataException("v2 session requires hello.");
            Publish(stream);
            LocalEnvelope registry = await V2Protocol.ReadLocalAsync(stream, token); if (registry.Type != LocalType.ChannelRegistry) throw new InvalidDataException("v2 session requires an approved channel registry."); PublishRegistry(stream, LocalBodies.ChannelRegistry(registry.Body));
            LocalEnvelope initial = await V2Protocol.ReadLocalAsync(stream, token); if (initial.Type != LocalType.ReceiverState) throw new InvalidDataException("v2 session requires receiver state.");
            (ulong initialGeneration, short initialThreshold, short initialTone, string initialProfile) = LocalBodies.ReceiverState(initial.Body); PublishState(stream, initialGeneration, initialThreshold, initialTone, initialProfile);
            while (!token.IsCancellationRequested)
            {
                LocalEnvelope message = await V2Protocol.ReadLocalAsync(stream, token);
                if (message.Type == LocalType.Audio) { (ulong generation, ulong index, byte[] pcm) = LocalBodies.Audio(message.Body); if (AcceptAudio(stream, generation)) await _audio(generation, index, pcm); continue; }
                if (message.Type == LocalType.ReceiverState) { (ulong generation, short value, short tone, string profileId) = LocalBodies.ReceiverState(message.Body); PublishState(stream, generation, value, tone, profileId); continue; }
                if (message.Type == LocalType.SquelchApplied) { (ulong id, short value) = LocalBodies.Squelch(message.Body); Complete(id, value); continue; }
                if (message.Type == LocalType.ControlApplied) { (ulong id, ulong generation, short value, short tone) = LocalBodies.Control(message.Body); CompleteControl(id, generation, value, tone); continue; }
                if (message.Type == LocalType.ChannelResult) { CompleteChannel(stream, LocalBodies.ChannelResult(message.Body)); continue; }
                throw new InvalidDataException("v2 receiver sent an invalid direction message.");
            }
        }
        finally { Retire(stream); }
    }
    public async Task<bool> SetControlAsync(ulong requestId, ulong expectedGeneration, short value, short ctcssTenths, CancellationToken token)
    {
        if (requestId == 0 || expectedGeneration == 0 || value is < -1000 or > 0 || !CtcssTone.IsValidTenths(ctcssTenths)) return false;
        TaskCompletionSource<(ulong, short, short)> completion = new(TaskCreationOptions.RunContinuationsAsynchronously); NetworkStream stream;
        lock (_gate) { if (_stream is null || _state is null || _state.Generation != expectedGeneration || !_controlPending.TryAdd(requestId, completion)) return false; stream = _stream; }
        try { await _write.WaitAsync(token); try { lock (_gate) if (!ReferenceEquals(_stream, stream) || _state?.Generation != expectedGeneration) return false; await V2Protocol.WriteLocalAsync(stream, LocalType.SetControl, LocalBodies.Control(requestId, expectedGeneration, value, ctcssTenths), token); await stream.FlushAsync(token); } finally { _write.Release(); }
            using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(TimeSpan.FromMilliseconds(900)); return await completion.Task.WaitAsync(deadline.Token) == (expectedGeneration, value, ctcssTenths); }
        catch (OperationCanceledException) { return false; } catch (IOException) { return false; } catch (ObjectDisposedException) { return false; }
        finally { lock (_gate) if (_controlPending.TryGetValue(requestId, out TaskCompletionSource<(ulong, short, short)>? current) && ReferenceEquals(current, completion)) _controlPending.Remove(requestId); }
    }
    public async Task<bool> SetSquelchAsync(ulong requestId, short value, CancellationToken token)
    {
        ReceiverControlState? state = EffectiveState; return state is not null && await SetControlAsync(requestId, state.Generation, value, state.CtcssTenths, token);
    }
    public async Task<ChannelSwitchResult?> SetChannelAsync(ChannelSwitchRequest request, CancellationToken token)
    {
        if (request.RequestId == Guid.Empty || request.ExpectedGeneration == 0) return null;
        PendingChannel pending; NetworkStream? stream; bool send;
        lock (_gate)
        {
            if (_stream is null || _state is null) return null;
            stream = _stream;
            if (_channelPending.TryGetValue(request.RequestId, out PendingChannel? existing))
            {
                pending = existing; send = false;
            }
            else
            {
                if (!_channels.Any(channel => channel.ProfileId == request.ProfileId)) return null;
                pending = new(request, new(TaskCreationOptions.RunContinuationsAsynchronously)); _channelPending.Add(request.RequestId, pending); send = true;
                _channel = new(request.RequestId, "pending", request.ProfileId, null);
            }
        }
        try
        {
            if (send)
            {
                await _write.WaitAsync(token);
                try { lock (_gate) if (!ReferenceEquals(_stream, stream)) return null; await V2Protocol.WriteLocalAsync(stream, LocalType.SetChannel, LocalBodies.ChannelRequest(request), token); await stream.FlushAsync(token); }
                finally { _write.Release(); }
            }
            // This must outlive the receiver-owned bounded graph-start window
            // and its bounded failed-child disposal, otherwise a truthful
            // replacement failure is misprojected as receiver-unavailable.
            using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(TimeSpan.FromSeconds(14));
            return await pending.Completion.Task.WaitAsync(deadline.Token);
        }
        catch (OperationCanceledException) { return null; } catch (IOException) { return null; } catch (ObjectDisposedException) { return null; }
        finally { lock (_gate) if (_channelPending.TryGetValue(request.RequestId, out PendingChannel? current) && ReferenceEquals(current, pending)) _channelPending.Remove(request.RequestId); }
    }
    private void Publish(NetworkStream stream)
    {
        NetworkStream? prior;
        lock (_gate)
        {
            prior = _stream;
            _stream = stream;
            _state = null;
            _channels = [];
            _channel = new(null, "unavailable", null, null);
            CancelPendingLocked();
        }
        _stateChanged(null);
        if (prior is not null && !ReferenceEquals(prior, stream)) prior.Dispose();
    }
    private void Retire(NetworkStream stream)
    {
        lock (_gate)
        {
            if (!ReferenceEquals(_stream, stream)) return;
            _stream = null;
            _state = null;
            _channels = [];
            _channel = new(null, "unavailable", null, null);
            CancelPendingLocked();
        }
        _stateChanged(null);
    }
    private void PublishRegistry(NetworkStream stream, IReadOnlyList<ApprovedChannelProjection> channels)
    {
        lock (_gate) { if (!ReferenceEquals(_stream, stream) || _state is not null) return; _channels = channels.ToArray(); }
    }
    private void PublishState(NetworkStream stream, ulong generation, short value, short tone, string profileId)
    {
        ReceiverControlState state;
        lock (_gate)
        {
            if (!ReferenceEquals(_stream, stream) || !_channels.Any(channel => channel.ProfileId == profileId) || _state is { } prior && generation != checked(prior.Generation + 1)) throw new InvalidDataException("Receiver generation did not advance exactly once.");
            state = new(generation, value, tone, profileId); _state = state;
            if (_channel.Status == "unavailable") _channel = new(null, "applied", null, null);
        }
        _stateChanged(state);
    }
    private bool AcceptAudio(NetworkStream stream, ulong generation) { lock (_gate) return ReferenceEquals(_stream, stream) && _state?.Generation == generation; }
    private void Complete(ulong id, short value) { }
    private void CompleteControl(ulong id, ulong generation, short value, short tone)
    {
        lock (_gate)
            if (_state is { } state && state.Generation == generation && _controlPending.TryGetValue(id, out TaskCompletionSource<(ulong, short, short)>? completion)) { _state = state with { ThresholdTenths = value, CtcssTenths = tone }; completion.TrySetResult((generation, value, tone)); }
    }
    private void CompleteChannel(NetworkStream stream, ChannelSwitchResult result)
    {
        lock (_gate)
        {
            if (!ReferenceEquals(_stream, stream) || _state is not { } state || !_channelPending.TryGetValue(result.RequestId, out PendingChannel? pending)) throw new InvalidDataException("Channel result did not match a pending request.");
            bool current = state.Generation == result.Generation && state.ProfileId == result.ProfileId && state.ThresholdTenths == result.ThresholdTenths && state.CtcssTenths == result.CtcssTenths;
            if (!current && result.Generation >= state.Generation) throw new InvalidDataException("Channel result did not match receiver state.");
            string status = result.Code == ChannelSwitchCode.Applied ? "applied" : "rejected";
            _channel = new(result.RequestId, status, null, result.Code switch { ChannelSwitchCode.UnknownProfile => "unknown-profile", ChannelSwitchCode.StaleGeneration => "stale-generation", ChannelSwitchCode.ReplacementFailed => "replacement-failed", ChannelSwitchCode.RequestConflict => "request-conflict", _ => null });
            if (_channelHistory.TryGetValue(result.RequestId, out ChannelSwitchResult? prior) && prior != result) throw new InvalidDataException("Channel request identity changed its outcome.");
            if (_channelHistory.Count < 256) _channelHistory.TryAdd(result.RequestId, result);
            pending.Completion.TrySetResult(result);
        }
    }
    private void CancelPendingLocked()
    {
        foreach (TaskCompletionSource<(ulong, short, short)> completion in _controlPending.Values) completion.TrySetCanceled();
        foreach (PendingChannel pending in _channelPending.Values) pending.Completion.TrySetCanceled();
        _controlPending.Clear();
        _channelPending.Clear();
    }
    public ValueTask DisposeAsync()
    {
        NetworkStream? stream;
        lock (_gate) { stream = _stream; _stream = null; _state = null; _channels = []; _channel = new(null, "unavailable", null, null); CancelPendingLocked(); }
        _stateChanged(null); stream?.Dispose(); _listener?.Dispose(); _write.Dispose(); if (File.Exists(_path)) File.Delete(_path); string? dir = System.IO.Path.GetDirectoryName(_path); if (dir is not null && Directory.Exists(dir) && !Directory.EnumerateFileSystemEntries(dir).Any()) Directory.Delete(dir); return ValueTask.CompletedTask;
    }
    private sealed record PendingChannel(ChannelSwitchRequest Request, TaskCompletionSource<ChannelSwitchResult> Completion);
}
public static class V2WebSocket
{
    // This is deliberately a small, user-actionable taxonomy. It is safe to
    // expose to the portal/test seam without disclosing control payloads or
    // browser credentials.
    public enum CloseCode { PeerClosed, MalformedFraming, InvalidPlayedSequence, ExcessiveCredit, LaggingListener, ReceiverGeneration }

    public static async Task RunAsync(WebSocket socket, BrowserListener listener, CancellationToken token, Action<CloseCode>? closed = null)
    {
        CloseCode? closeCode = null;
        try
        {
            byte[] control = new byte[128]; int controlLength = 0; Task<WebSocketReceiveResult>? receive = null;
            while (socket.State == WebSocketState.Open && !token.IsCancellationRequested)
            {
                if (listener.GenerationRetired) { closeCode = CloseCode.ReceiverGeneration; await socket.CloseOutputAsync((WebSocketCloseStatus)1012, "receiver-generation", token); return; }
                receive ??= socket.ReceiveAsync(new ArraySegment<byte>(control, controlLength, control.Length - controlLength), token);
                while (listener.TrySend(out byte[]? packet)) { using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(TimeSpan.FromMilliseconds(250)); try { await socket.SendAsync(packet!, WebSocketMessageType.Binary, true, deadline.Token); } catch (OperationCanceledException) when (!token.IsCancellationRequested) { closeCode = CloseCode.LaggingListener; await socket.CloseOutputAsync((WebSocketCloseStatus)1013, "lagging listener", token); return; } }
                if (await Task.WhenAny(receive, Task.Delay(5, token)) != receive) continue;
                WebSocketReceiveResult result = await receive; receive = null;
                if (result.MessageType == WebSocketMessageType.Close) { closeCode = CloseCode.PeerClosed; return; }
                if (result.MessageType != WebSocketMessageType.Text || result.Count > control.Length - controlLength)
                {
                    closeCode = CloseCode.MalformedFraming; await socket.CloseOutputAsync(WebSocketCloseStatus.PolicyViolation, "malformed-control", token); return;
                }
                controlLength += result.Count;
                if (!result.EndOfMessage)
                {
                    // A full non-final receive proves this control is over the
                    // protocol's 128-byte limit before another receive would
                    // need a zero-length buffer.
                    if (controlLength == control.Length) { closeCode = CloseCode.MalformedFraming; await socket.CloseOutputAsync(WebSocketCloseStatus.PolicyViolation, "malformed-control", token); return; }
                    continue;
                }
                BrowserControlOutcome outcome;
                try { outcome = controlLength == 0 ? throw new InvalidDataException() : listener.Control(V2Protocol.ParseClient(control.AsSpan(0, controlLength))); }
                catch (Exception error) when (error is InvalidDataException or JsonException) { closeCode = CloseCode.MalformedFraming; await socket.CloseOutputAsync(WebSocketCloseStatus.PolicyViolation, "malformed-control", token); return; }
                controlLength = 0;
                // Old worklet callbacks can arrive after the page has advanced
                // the same socket to a re-prime attempt.  They are harmless and
                // must not close or mutate the current attempt.
                if (outcome == BrowserControlOutcome.StaleAttempt) continue;
                if (outcome is BrowserControlOutcome.IgnoredInvalidPlayedSequence or BrowserControlOutcome.IgnoredExcessiveCredit) continue;
                if (outcome is BrowserControlOutcome.InvalidPlayedSequence or BrowserControlOutcome.ExcessiveCredit)
                {
                    closeCode = outcome == BrowserControlOutcome.InvalidPlayedSequence ? CloseCode.InvalidPlayedSequence : CloseCode.ExcessiveCredit;
                    await socket.CloseOutputAsync(WebSocketCloseStatus.PolicyViolation, outcome == BrowserControlOutcome.InvalidPlayedSequence ? "invalid-played" : "excess-credit", token); return;
                }
                if (listener.Lagging) { closeCode = CloseCode.LaggingListener; await socket.CloseOutputAsync((WebSocketCloseStatus)1013, "listener-lagging", token); return; }
            }
        }
        finally { if (closeCode is CloseCode code) closed?.Invoke(code); }
    }
}
