using T28.V2.Receiver;
using T28.V2.Contracts;

using CancellationTokenSource stop = new();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
try
{
    string runtime = Environment.GetEnvironmentVariable("T28_V2_RUNTIME") ?? throw new InvalidOperationException("T28_V2_RUNTIME is required.");
    ReceiverProfileRegistry profiles = ReceiverProfileRegistry.LoadFromEnvironment();
    // Only this receiver owner resolves approved child inputs. The local server
    // and browser receive opaque profile identifiers and friendly labels.
    FleetAgentConfiguration? fleet = FleetAgentConfiguration.Parse(
        Environment.GetEnvironmentVariable("T28_V2_FLEET_URL"),
        Environment.GetEnvironmentVariable("T28_V2_FLEET_RECEIVER_ID"),
        Environment.GetEnvironmentVariable("T28_V2_FLEET_AGENT_TOKEN"));
    if (fleet is not null)
    {
        int heartbeatMilliseconds = int.TryParse(Environment.GetEnvironmentVariable("T28_V2_FLEET_HEARTBEAT_MS"), out int configuredHeartbeat) && configuredHeartbeat is >= 100 and <= 5000 ? configuredHeartbeat : 5000;
        // Control-plane hello/heartbeat/commands/snapshot/ack must not share an
        // infinite HttpClient timeout. A hung TCP then never returns, the
        // heartbeat loop goes silent, and systemd Type=simple never restarts
        // the still-alive process. Audio on this tree is discrete POSTs, not
        // one session-long Opus stream, so it can use the same finite deadline.
        using HttpClient client = new() { BaseAddress = fleet.ServerUrl, Timeout = TimeSpan.FromSeconds(30) };
        await using FleetReceiverAgent agent = new(profiles, new GnuRadioChildFactory(), client, fleet.ReceiverId, fleet.AgentToken, TimeSpan.FromMilliseconds(heartbeatMilliseconds));
        await agent.RunAsync(stop.Token);
        return 0;
    }
    await using V2ReceiverRunner runner = new(new V2ReceiverOwner(profiles, new GnuRadioChildFactory()), Path.Combine(runtime, "receiver.v2.sock"));
    if (Environment.GetEnvironmentVariable("T28_V2_TEST_UNEXPECTED_FAULT") == "1") throw new InvalidOperationException("test fault");
    await runner.RunAsync(stop.Token);
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
catch
{
    // Keep faults concise: runner disposal stops its only child/session before
    // this non-zero return reaches the external no-core process boundary.
    Console.Error.WriteLine("t28 v2 receiver stopped unexpectedly");
    return 1;
}
return 0;
