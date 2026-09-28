using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.Sockets;
using System.IO.Pipes;
using System.Text;
using System.Threading.Channels;
using T28.V2.Contracts;

namespace T28.V2.Receiver;

public interface IGnuRadioV2
{
    int ProcessId { get; }
    Task ReadyAsync(CancellationToken token);
    Task<(short Threshold, short CtcssTenths)> SetControlAsync(ulong requestId, short threshold, short ctcssTenths, CancellationToken token);
    IAsyncEnumerable<(ulong FirstSample, byte[] Pcm)> ReadPcmAsync(CancellationToken token);
    Task StopAsync(CancellationToken token);
}

/// <summary>Owns exactly one child and keeps its acknowledged control state when an acknowledgement fails.</summary>
public sealed class V2Receiver(IGnuRadioV2 child)
{
    private readonly SemaphoreSlim _control = new(1, 1);
    private readonly PcmMediaPacer _pacer = new();
    private short _threshold = -1000, _ctcssTenths = CtcssTone.Default;
    public short Threshold => _threshold;
    public short CtcssTenths => _ctcssTenths;
    public int ChildProcessId => child.ProcessId;
    public LiveQueue<(ulong FirstSample, byte[] Pcm)> SessionQueue { get; } = new();
    public async Task<bool> SetControlAsync(ulong requestId, short proposed, short ctcssTenths, TimeSpan timeout, CancellationToken token)
    {
        if (requestId == 0 || proposed is < -1000 or > 0 || !CtcssTone.IsValidTenths(ctcssTenths)) return false;
        await _control.WaitAsync(token); try
        {
            using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(timeout);
            (short applied, short appliedTone) = await child.SetControlAsync(requestId, proposed, ctcssTenths, deadline.Token);
            if (applied != proposed || appliedTone != ctcssTenths) return false; _threshold = proposed; _ctcssTenths = ctcssTenths; return true;
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested) { return false; }
        catch (Exception) when (!token.IsCancellationRequested) { return false; }
        finally { _control.Release(); }
    }
    public Task<bool> SetThresholdAsync(ulong requestId, short proposed, TimeSpan timeout, CancellationToken token) => SetControlAsync(requestId, proposed, _ctcssTenths, timeout, token);
    public async Task DrainPcmAsync(CancellationToken token)
    {
        Task pace = _pacer.PaceAsync((index, pcm) => SessionQueue.Push((index, pcm)), token);
        Exception? failure = null;
        try
        {
            await foreach ((ulong index, byte[] pcm) in child.ReadPcmAsync(token)) await _pacer.EnqueueAsync(index, pcm, token);
        }
        catch (Exception error)
        {
            failure = error;
            throw;
        }
        finally
        {
            _pacer.Complete(failure);
            await pace;
        }
    }
    public async Task StopAsync(CancellationToken token) { SessionQueue.Clear(); await child.StopAsync(token); _control.Dispose(); }
}

/// <summary>
/// The only burst-to-live boundary.  GNU Radio stdout may arrive in pipe-sized
/// bursts, so this fixed 640 ms inbox owns validated packets before the first
/// five-packet live queue and releases one immutable 20 ms packet per media
/// clock tick.  On a real source starvation it emits nothing; downstream
/// AudioWorklet underrun zero-fill remains the truthful live result.
/// </summary>
public sealed class PcmMediaPacer
{
    public const int Capacity = 32;
    public static readonly TimeSpan PacketDuration = TimeSpan.FromMilliseconds(20);
    private static readonly TimeSpan MaximumLateness = TimeSpan.FromMilliseconds(20);
    private readonly Channel<(ulong FirstSample, byte[] Pcm)> _source = Channel.CreateBounded<(ulong, byte[])>(new BoundedChannelOptions(Capacity) { SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait });
    private readonly Func<TimeSpan> _now;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    public PcmMediaPacer(Func<TimeSpan>? now = null, Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        long origin = Stopwatch.GetTimestamp();
        _now = now ?? (() => Stopwatch.GetElapsedTime(origin));
        _delay = delay ?? Task.Delay;
    }

    public ValueTask EnqueueAsync(ulong firstSample, byte[] pcm, CancellationToken token)
    {
        if (pcm.Length != V2Protocol.PcmBytes) throw new InvalidDataException("GNU Radio adapter emitted non-20ms PCM.");
        // This copy is the source boundary's immutable packet ownership.  No
        // subsequent pipe read can overwrite a queued source frame.
        return _source.Writer.WriteAsync((firstSample, pcm.ToArray()), token);
    }

    public void Complete(Exception? error = null) => _source.Writer.TryComplete(error);

    public async Task PaceAsync(Action<ulong, byte[]> output, CancellationToken token)
    {
        TimeSpan? next = null;
        await foreach ((ulong index, byte[] pcm) in _source.Reader.ReadAllAsync(token))
        {
            TimeSpan now = _now();
            TimeSpan due = next ?? now;
            // Do not catch up a delayed source by replaying a stored burst.
            // The next real packet restarts the media edge after an underrun.
            if (now > due + MaximumLateness) due = now;
            TimeSpan wait = due - now;
            if (wait > TimeSpan.Zero) await _delay(wait, token);
            token.ThrowIfCancellationRequested();
            output(index, pcm);
            next = due + PacketDuration;
        }
    }
}

/// <summary>Shell-free adapter: stdout is fixed PCM records and a dedicated pipe carries one exact graph-readiness marker followed by exact control acknowledgements.
/// GNU Radio/osmosdr stderr is drained independently and is never a control input.</summary>
public sealed class GnuRadioProcessAdapter : IGnuRadioV2
{
    private readonly Process _process;
    private readonly SemaphoreSlim _control = new(1, 1);
    private readonly AnonymousPipeServerStream _controlPipe;
    private readonly StreamReader _controlReplies;
    private readonly Task _diagnosticDrain;
    private readonly Channel<(ulong FirstSample, byte[] Pcm)> _pcm = Channel.CreateBounded<(ulong, byte[])>(new BoundedChannelOptions(4) { SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait });
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly CancellationTokenSource _pumpStop = new();
    private readonly object _lifecycle = new();
    private readonly Task _pump;
    private Task? _stopping;
    private ulong _nextSample;
    public int ProcessId => _process.Id;
    public GnuRadioProcessAdapter(string executable, string adapterPath, string profilePath, bool synthetic, string? syntheticControlMode = null, string? syntheticReadinessMode = null, bool syntheticDiagnostics = false, int? syntheticBurstMilliseconds = null)
    {
        if (!Path.IsPathFullyQualified(profilePath) || !File.Exists(profilePath)) throw new ArgumentException("v2 profile path must be an existing absolute task-owned file.", nameof(profilePath));
        ProcessStartInfo start = new(executable) { UseShellExecute = false, RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        if (!Path.IsPathFullyQualified(adapterPath) || !File.Exists(adapterPath)) throw new ArgumentException("v2 adapter must be an existing absolute task-owned file.", nameof(adapterPath));
        _controlPipe = new AnonymousPipeServerStream(PipeDirection.In, HandleInheritability.Inheritable);
        _controlReplies = new StreamReader(_controlPipe, Encoding.UTF8, false, 128, leaveOpen: true);
        start.ArgumentList.Add(adapterPath); start.ArgumentList.Add("--v2-fixed-profile"); start.ArgumentList.Add(profilePath); start.ArgumentList.Add("--power-squelch-tenths"); start.ArgumentList.Add("-1000"); start.ArgumentList.Add("--pcm-stdout"); start.ArgumentList.Add("--control-fd"); start.ArgumentList.Add(_controlPipe.GetClientHandleAsString());
        if (synthetic) start.ArgumentList.Add("--synthetic-test");
        if (!string.IsNullOrEmpty(syntheticControlMode)) { start.ArgumentList.Add("--synthetic-control-mode"); start.ArgumentList.Add(syntheticControlMode); }
        if (!string.IsNullOrEmpty(syntheticReadinessMode)) { start.ArgumentList.Add("--synthetic-readiness-mode"); start.ArgumentList.Add(syntheticReadinessMode); }
        if (syntheticDiagnostics) start.ArgumentList.Add("--synthetic-diagnostics");
        if (syntheticBurstMilliseconds is { } burst)
        {
            if (burst is < 500 or > 550) throw new ArgumentOutOfRangeException(nameof(syntheticBurstMilliseconds));
            start.ArgumentList.Add("--synthetic-burst-ms"); start.ArgumentList.Add(burst.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        try
        {
            _process = Process.Start(start) ?? throw new InvalidOperationException("Unable to start v2 GNU Radio child.");
            _controlPipe.DisposeLocalCopyOfClientHandle();
            _diagnosticDrain = DrainDiagnosticsAsync(_process.StandardError);
            _pump = PumpPcmAsync(_pumpStop.Token);
        }
        catch
        {
            _controlReplies.Dispose(); _controlPipe.Dispose(); throw;
        }
    }
    public async Task ReadyAsync(CancellationToken token)
    {
        await _control.WaitAsync(token);
        try
        {
            if (_ready.Task.IsCompleted) { await _ready.Task.WaitAsync(token); return; }
            if (_process.HasExited) throw new InvalidOperationException("v2 GNU Radio child is unavailable before readiness.");
            string? marker = await _controlReplies.ReadLineAsync(token);
            if (marker != "ready v2") throw new InvalidDataException("GNU Radio returned an invalid v2 readiness marker.");
            _ready.TrySetResult();
        }
        catch (Exception error)
        {
            _ready.TrySetException(error);
            throw;
        }
        finally { _control.Release(); }
    }
    public async Task<(short Threshold, short CtcssTenths)> SetControlAsync(ulong requestId, short threshold, short ctcssTenths, CancellationToken token)
    {
        await _control.WaitAsync(token); try
        {
            if (_ready.Task.Status != TaskStatus.RanToCompletion) throw new InvalidOperationException("v2 GNU Radio child has not completed readiness.");
            if (_process.HasExited) throw new InvalidOperationException("v2 GNU Radio child is unavailable.");
            using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(TimeSpan.FromMilliseconds(500));
            if (requestId == 0 || !CtcssTone.IsValidTenths(ctcssTenths)) throw new InvalidDataException("v2 control acknowledgement requires valid selection.");
            await _process.StandardInput.WriteLineAsync($"set_control {requestId} {threshold} {ctcssTenths}".AsMemory(), deadline.Token); await _process.StandardInput.FlushAsync(deadline.Token);
            string? reply = await _controlReplies.ReadLineAsync(deadline.Token);
            if (reply != $"applied {requestId} {threshold} {ctcssTenths}") throw new InvalidDataException("GNU Radio returned an invalid v2 control acknowledgement."); return (threshold, ctcssTenths);
        }
        finally { _control.Release(); }
    }
    public async IAsyncEnumerable<(ulong FirstSample, byte[] Pcm)> ReadPcmAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken token)
    {
        await foreach ((ulong firstSample, byte[] pcm) in _pcm.Reader.ReadAllAsync(token)) yield return (firstSample, pcm);
    }
    private async Task PumpPcmAsync(CancellationToken token)
    {
        Exception? failure = null;
        try
        {
            byte[] pcm = new byte[V2Protocol.PcmBytes];
            while (!token.IsCancellationRequested)
            {
                int read = 0;
                while (read < pcm.Length)
                {
                    int count = await _process.StandardOutput.BaseStream.ReadAsync(pcm.AsMemory(read), token);
                    if (count == 0)
                    {
                        if (read != 0) throw new InvalidDataException("GNU Radio ended within a PCM packet.");
                        return;
                    }
                    read += count;
                }
                byte[] packet = pcm.ToArray(); ulong firstSample = _nextSample; _nextSample += V2Protocol.SamplesPerPacket;
                await _pcm.Writer.WriteAsync((firstSample, packet), token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception error) { failure = error; _ready.TrySetException(error); }
        finally { _pcm.Writer.TryComplete(failure); }
    }
    public Task StopAsync(CancellationToken token)
    {
        lock (_lifecycle) return _stopping ??= StopCoreAsync();
    }
    private async Task StopCoreAsync()
    {
        try
        {
            _process.StandardInput.Close();
            if (!_process.HasExited)
            {
                using CancellationTokenSource deadline = new(TimeSpan.FromSeconds(2));
                try { await _process.WaitForExitAsync(deadline.Token); }
                catch (OperationCanceledException) { if (!_process.HasExited) _process.Kill(entireProcessTree: true); await _process.WaitForExitAsync(); }
            }
        }
        finally
        {
            _pumpStop.Cancel();
            try { await _pump; } catch { }
            try { await _diagnosticDrain; } catch { }
            _controlReplies.Dispose(); _controlPipe.Dispose(); _control.Dispose(); _pumpStop.Dispose(); _process.Dispose();
        }
    }
    private static async Task DrainDiagnosticsAsync(StreamReader diagnostics)
    {
        char[] buffer = new char[256];
        while (await diagnostics.ReadAsync(buffer.AsMemory()) != 0) { }
    }
}

public sealed record ReceiverOwnedState(ulong Generation, string ProfileId, short ThresholdTenths, short CtcssTenths);
public sealed record ReceiverAudioPacket(ulong Generation, ulong FirstSample, byte[] Pcm);

/// <summary>
/// The only owner of approved receive children. Replacements become active only
/// after bounded explicit graph readiness and an exact control acknowledgement. Every old
/// drain checks active object identity before it can publish a packet.
/// </summary>
public sealed class V2ReceiverOwner : IAsyncDisposable
{
    private const int RequestHistoryLimit = 256;
    // GNU Radio graph construction on a development host can exceed one second before
    // producing any carrier/CTCSS-gated PCM. The child emits its exact marker
    // only after graph.start(); this remains a bounded startup window.
    private static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(10), ControlTimeout = TimeSpan.FromMilliseconds(750), StopTimeout = TimeSpan.FromSeconds(3);
    private readonly ReceiverProfileRegistry _profiles;
    private readonly IReceiverChildFactory _factory;
    private readonly SemaphoreSlim _change = new(1, 1);
    private readonly object _stateGate = new();
    private readonly Dictionary<Guid, ChannelSwitchResult> _history = [];
    private ActiveChild? _active;
    private long _nextChildControl = 10_000;
    private int _disposed;

    public V2ReceiverOwner(ReceiverProfileRegistry profiles, IReceiverChildFactory factory) { _profiles = profiles; _factory = factory; }
    public IReadOnlyList<ApprovedChannelProjection> PublicProfiles => _profiles.PublicProfiles;
    public LiveQueue<ReceiverAudioPacket> SessionQueue { get; } = new();
    public ReceiverOwnedState? State { get { lock (_stateGate) return _active?.Snapshot; } }
    public bool ActiveDrainFailed { get { lock (_stateGate) return _active is { Drain.IsFaulted: true } || _active is { Drain.IsCompletedSuccessfully: true }; } }

    public async Task InitializeAsync(CancellationToken token)
    {
        await _change.WaitAsync(token);
        try
        {
            if (_active is not null) return;
            PreparedChild prepared = await PrepareAsync(_profiles.Active, token);
            ActiveChild active = new(1, prepared.Profile, prepared.Child, prepared.Threshold, prepared.Tone);
            active.Drain = DrainAsync(active);
            lock (_stateGate) _active = active;
        }
        finally { _change.Release(); }
    }

    public async Task<bool> SetControlAsync(ulong requestId, ulong expectedGeneration, short threshold, short tone, CancellationToken token)
    {
        if (requestId == 0 || expectedGeneration == 0 || threshold is < -1000 or > 0 || !CtcssTone.IsValidTenths(tone)) return false;
        await _change.WaitAsync(token);
        try
        {
            ActiveChild? active; lock (_stateGate) active = _active;
            if (active is null || active.Generation != expectedGeneration) return false;
            using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(ControlTimeout);
            try
            {
                (short applied, short appliedTone) = await active.Child.SetControlAsync(requestId, threshold, tone, deadline.Token);
                if (applied != threshold || appliedTone != tone) return false;
                lock (_stateGate)
                {
                    if (!ReferenceEquals(_active, active)) return false;
                    active.Threshold = threshold; active.Tone = tone;
                }
                return true;
            }
            catch (Exception error) when (!token.IsCancellationRequested && (error is OperationCanceledException or IOException or InvalidDataException or InvalidOperationException)) { return false; }
        }
        finally { _change.Release(); }
    }

    public async Task<ChannelSwitchResult> SwitchAsync(ChannelSwitchRequest request, CancellationToken token)
    {
        if (request.RequestId == Guid.Empty) throw new InvalidDataException("A channel request identity is required.");
        await _change.WaitAsync(token);
        try
        {
            if (_history.TryGetValue(request.RequestId, out ChannelSwitchResult? replay)) return replay;
            ActiveChild active; lock (_stateGate) active = _active ?? throw new InvalidOperationException("The receiver owner is not initialized.");
            if (_history.Count >= RequestHistoryLimit) return Conflict(request, active);
            if (!_profiles.TryGet(request.ProfileId, out ApprovedReceiverProfile? selected) || selected is null) return Remember(request.RequestId, Rejected(request, ChannelSwitchCode.UnknownProfile, active));
            if (request.ExpectedGeneration != active.Generation) return Remember(request.RequestId, Rejected(request, ChannelSwitchCode.StaleGeneration, active));
            if (string.Equals(request.ProfileId, active.Profile.ProfileId, StringComparison.Ordinal)) return Remember(request.RequestId, Applied(request.RequestId, active));

            PreparedChild prepared;
            try { prepared = await PrepareAsync(selected, token); }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
            catch { return Remember(request.RequestId, Rejected(request, ChannelSwitchCode.ReplacementFailed, active)); }

            ActiveChild replacement = new(checked(active.Generation + 1), prepared.Profile, prepared.Child, prepared.Threshold, prepared.Tone);
            replacement.Drain = DrainAsync(replacement);
            lock (_stateGate)
            {
                if (!ReferenceEquals(_active, active)) throw new InvalidOperationException("Receiver ownership changed outside the mutation gate.");
                _active = replacement;
                SessionQueue.Clear();
            }
            active.Stop.Cancel();
            await StopAsync(active);
            return Remember(request.RequestId, Applied(request.RequestId, replacement));
        }
        finally { _change.Release(); }
    }

    private async Task<PreparedChild> PrepareAsync(ApprovedReceiverProfile profile, CancellationToken token)
    {
        IGnuRadioV2? child = null;
        try
        {
            using CancellationTokenSource ready = CancellationTokenSource.CreateLinkedTokenSource(token); ready.CancelAfter(ReadyTimeout);
            child = await _factory.CreateAsync(profile, ready.Token);
            await child.ReadyAsync(ready.Token);
            ulong controlId = (ulong)Interlocked.Increment(ref _nextChildControl);
            (short threshold, short tone) = await child.SetControlAsync(controlId, profile.ThresholdTenths, profile.CtcssTenths, ready.Token);
            if (threshold != profile.ThresholdTenths || tone != profile.CtcssTenths) throw new InvalidDataException("Prepared child did not acknowledge its approved controls.");
            return new(profile, child, threshold, tone);
        }
        catch
        {
            if (child is not null) await StopPreparedAsync(child);
            throw;
        }
    }

    private async Task DrainAsync(ActiveChild active)
    {
        PcmMediaPacer pacer = new(); Exception? failure = null;
        Task pace = pacer.PaceAsync((firstSample, pcm) =>
        {
            lock (_stateGate) if (ReferenceEquals(_active, active) && !active.Stop.IsCancellationRequested) SessionQueue.Push(new(active.Generation, firstSample, pcm));
        }, active.Stop.Token);
        try { await foreach ((ulong firstSample, byte[] pcm) in active.Child.ReadPcmAsync(active.Stop.Token)) await pacer.EnqueueAsync(firstSample, pcm, active.Stop.Token); }
        catch (OperationCanceledException) when (active.Stop.IsCancellationRequested) { }
        catch (Exception error) { failure = error; throw; }
        finally
        {
            pacer.Complete(failure);
            try { await pace; } catch (OperationCanceledException) when (active.Stop.IsCancellationRequested) { }
        }
    }

    private static ChannelSwitchResult Applied(Guid requestId, ActiveChild active) => new(requestId, ChannelSwitchCode.Applied, active.Generation, active.Profile.ProfileId, active.Threshold, active.Tone);
    private static ChannelSwitchResult Rejected(ChannelSwitchRequest request, ChannelSwitchCode code, ActiveChild active) => new(request.RequestId, code, active.Generation, active.Profile.ProfileId, active.Threshold, active.Tone);
    private static ChannelSwitchResult Conflict(ChannelSwitchRequest request, ActiveChild active) => Rejected(request, ChannelSwitchCode.RequestConflict, active);
    private ChannelSwitchResult Remember(Guid requestId, ChannelSwitchResult result) { _history.Add(requestId, result); return result; }
    private static async Task StopPreparedAsync(IGnuRadioV2 child)
    {
        try { await child.StopAsync(CancellationToken.None).WaitAsync(StopTimeout); } catch { }
    }
    private static async Task StopAsync(ActiveChild active)
    {
        await active.Child.StopAsync(CancellationToken.None).WaitAsync(StopTimeout);
        try { await active.Drain.WaitAsync(StopTimeout); } catch (OperationCanceledException) when (active.Stop.IsCancellationRequested) { } catch (Exception) when (active.Stop.IsCancellationRequested) { }
        active.Stop.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        await _change.WaitAsync();
        try
        {
            ActiveChild? active; lock (_stateGate) { active = _active; _active = null; SessionQueue.Clear(); }
            if (active is not null) { active.Stop.Cancel(); await StopAsync(active); }
        }
        finally { _change.Release(); _change.Dispose(); }
    }

    private sealed record PreparedChild(ApprovedReceiverProfile Profile, IGnuRadioV2 Child, short Threshold, short Tone);
    private sealed class ActiveChild(ulong generation, ApprovedReceiverProfile profile, IGnuRadioV2 child, short threshold, short tone)
    {
        public ulong Generation { get; } = generation;
        public ApprovedReceiverProfile Profile { get; } = profile;
        public IGnuRadioV2 Child { get; } = child;
        public short Threshold { get; set; } = threshold;
        public short Tone { get; set; } = tone;
        public CancellationTokenSource Stop { get; } = new();
        public Task Drain { get; set; } = Task.CompletedTask;
        public ReceiverOwnedState Snapshot => new(Generation, Profile.ProfileId, Threshold, Tone);
    }
}

/// <summary>The reconnecting UDS client for one receiver-owned child generation.</summary>
public sealed class V2ReceiverRunner : IAsyncDisposable
{
    private readonly V2ReceiverOwner _owner;
    private readonly string _socketPath;
    private readonly SemaphoreSlim _write = new(1, 1);
    private long _wireGeneration;

    public V2ReceiverRunner(V2ReceiverOwner owner, string socketPath) { _owner = owner; _socketPath = socketPath; }
    public async Task RunAsync(CancellationToken token)
    {
        await _owner.InitializeAsync(token);
        while (!token.IsCancellationRequested && !_owner.ActiveDrainFailed)
        {
            try
            {
                using Socket socket = new(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                await socket.ConnectAsync(new UnixDomainSocketEndPoint(_socketPath), token);
                await using NetworkStream stream = new(socket, ownsSocket: false);
                await PublishInitialAsync(stream, token);
                using CancellationTokenSource sessionStop = CancellationTokenSource.CreateLinkedTokenSource(token);
                Task controls = ReadControlsAsync(stream, sessionStop.Token);
                while (!token.IsCancellationRequested && !controls.IsCompleted && !_owner.ActiveDrainFailed)
                {
                    if (_owner.SessionQueue.TryTake(out ReceiverAudioPacket? packet) && packet is not null)
                    {
                        if (packet.Generation == (ulong)Interlocked.Read(ref _wireGeneration)) await WriteAsync(stream, LocalType.Audio, LocalBodies.Audio(packet.Generation, packet.FirstSample, packet.Pcm), token);
                    }
                    else await Task.Delay(2, token);
                }
                if (_owner.ActiveDrainFailed || token.IsCancellationRequested) sessionStop.Cancel();
                try { await controls; } catch (OperationCanceledException) when (sessionStop.IsCancellationRequested) { }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { break; }
            catch (SocketException) { await Task.Delay(25, token); }
            catch (EndOfStreamException) { await Task.Delay(25, token); }
            catch (IOException) { await Task.Delay(25, token); }
        }
        if (_owner.ActiveDrainFailed) throw new InvalidOperationException("The active receive child stopped unexpectedly.");
    }

    private async Task PublishInitialAsync(NetworkStream stream, CancellationToken token)
    {
        ReceiverOwnedState state = _owner.State ?? throw new InvalidOperationException("Receiver owner state is unavailable.");
        await _write.WaitAsync(token);
        try
        {
            await V2Protocol.WriteLocalAsync(stream, LocalType.Hello, LocalBodies.Hello(), token);
            await V2Protocol.WriteLocalAsync(stream, LocalType.ChannelRegistry, LocalBodies.ChannelRegistry(_owner.PublicProfiles), token);
            await V2Protocol.WriteLocalAsync(stream, LocalType.ReceiverState, LocalBodies.ReceiverState(state.Generation, state.ThresholdTenths, state.CtcssTenths, state.ProfileId), token);
            await stream.FlushAsync(token); Interlocked.Exchange(ref _wireGeneration, checked((long)state.Generation));
        }
        finally { _write.Release(); }
    }

    private async Task ReadControlsAsync(NetworkStream stream, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            LocalEnvelope command = await V2Protocol.ReadLocalAsync(stream, token);
            if (command.Type == LocalType.SetControl)
            {
                (ulong id, ulong generation, short value, short tone) = LocalBodies.Control(command.Body);
                if (!await _owner.SetControlAsync(id, generation, value, tone, token)) throw new InvalidDataException("v2 control not acknowledged by the active generation.");
                await WriteAsync(stream, LocalType.ControlApplied, LocalBodies.Control(id, generation, value, tone), token);
                continue;
            }
            if (command.Type == LocalType.SetChannel)
            {
                ChannelSwitchResult result = await _owner.SwitchAsync(LocalBodies.ChannelRequest(command.Body), token);
                await WriteChannelResultAsync(stream, result, token);
                continue;
            }
            throw new InvalidDataException("v2 receiver received invalid local command.");
        }
    }

    private async Task WriteChannelResultAsync(Stream stream, ChannelSwitchResult result, CancellationToken token)
    {
        await _write.WaitAsync(token);
        try
        {
            if (result.Code == ChannelSwitchCode.Applied && result.Generation != (ulong)Interlocked.Read(ref _wireGeneration))
            {
                await V2Protocol.WriteLocalAsync(stream, LocalType.ReceiverState, LocalBodies.ReceiverState(result.Generation, result.ThresholdTenths, result.CtcssTenths, result.ProfileId), token);
                Interlocked.Exchange(ref _wireGeneration, checked((long)result.Generation));
            }
            await V2Protocol.WriteLocalAsync(stream, LocalType.ChannelResult, LocalBodies.ChannelResult(result), token);
            await stream.FlushAsync(token);
        }
        finally { _write.Release(); }
    }

    private async Task WriteAsync(Stream stream, LocalType type, byte[] body, CancellationToken token)
    { await _write.WaitAsync(token); try { await V2Protocol.WriteLocalAsync(stream, type, body, token); await stream.FlushAsync(token); } finally { _write.Release(); } }
    public async ValueTask DisposeAsync() { await _owner.DisposeAsync(); _write.Dispose(); }
}
