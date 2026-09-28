#!/usr/bin/env python3
"""The v2 GNU Radio PCM/control child.

This module deliberately has no GNU Radio imports at module load time.  That
makes the fixed receive graph inspectable and testable without an SDR or the
GNU Radio runtime, while production mode still constructs exactly one graph.
"""
import argparse
import json
import math
import os
import select
import sys
import time
import threading
import numpy

SAMPLE_RATE = 48_000
SOURCE_RATE = 240_000
CHANNEL_BANDWIDTH = 12_500
PACKET_SAMPLES = 960
PCM_BYTES = PACKET_SAMPLES * 2
INITIAL_POWER_SQUELCH_TENTHS = -1000
MAX_THRESHOLD_TENTHS = 0
MIN_THRESHOLD_TENTHS = -1000
LISTENER_HPF_CUTOFF_HZ = 300
LISTENER_HPF_TAPS = 513
CTCSS_TONES = (67.0,69.3,71.9,74.4,77.0,79.7,82.5,85.4,88.5,91.5,94.8,97.4,100.0,103.5,107.2,110.9,114.8,118.8,123.0,127.3,131.8,136.5,141.3,146.2,151.4,156.7,159.8,162.2,165.5,167.9,171.3,173.8,177.3,179.9,183.5,186.2,189.9,192.8,196.6,199.5,203.5,206.5,210.7,218.1,225.7,229.1,233.6,241.8,250.3,254.1)
DEFAULT_CTCSS_TENTHS, NONE_CTCSS_TENTHS = 1622, -1
READINESS_MARKER = "ready v2"
# 100 ms, 20 ms overlapping phase-insensitive decisions.  Three positives
# attack; six negative decisions give a bounded 120 ms release once the
# analysis window has moved past a disappearing tone.
CTCSS_WINDOW, CTCSS_STEP, CTCSS_ATTACK, CTCSS_RELEASE = 4_800, PACKET_SAMPLES, 3, 6


def dsp_order():
    """The physical chain, expressed independently of installed GNU Radio."""
    return ("rtl-sdr-source", "channel-12500", "manual-power-squelch",
            "nbfm", "ctcss-detector-gate-pre-high-pass", "listener-high-pass", "pcm-s16le-48000")


def valid_ctcss_tenths(value):
    return value == NONE_CTCSS_TENTHS or any(round(tone * 10) == value for tone in CTCSS_TONES)


class CtcssGate:
    """Manual, fresh 48 kHz detector: quadrature energy plus selectivity.

    It never uses the retired v1 implementation.  Each decision uses the full
    window (phase agnostic), compares every standard alternative, and requires
    a meaningful selected-energy share to reject hum, voice and broadband noise.
    """
    def __init__(self, selected_tenths=DEFAULT_CTCSS_TENTHS):
        if not valid_ctcss_tenths(selected_tenths): raise ValueError("unsupported CTCSS selection")
        self.selected_tenths = selected_tenths; self.pending = []
        self.good = self.misses = 0; self.open = selected_tenths == NONE_CTCSS_TENTHS
    def set_selection(self, selected_tenths):
        if not valid_ctcss_tenths(selected_tenths): return False
        self.selected_tenths = selected_tenths; self.pending.clear(); self.good = self.misses = 0
        self.open = selected_tenths == NONE_CTCSS_TENTHS; return True
    @staticmethod
    def _energy(values, frequency):
        omega = 2 * math.pi * frequency / SAMPLE_RATE; coefficient = 2 * math.cos(omega); q1 = q2 = 0.0
        for value in values: q0 = float(value) + coefficient * q1 - q2; q2, q1 = q1, q0
        return q1*q1 + q2*q2 - coefficient*q1*q2
    def _present(self, values):
        if self.selected_tenths == NONE_CTCSS_TENTHS: return True
        selected = self.selected_tenths / 10.0; target = max(0.0, self._energy(values, selected))
        alternatives = max(max(0.0, self._energy(values, tone)) for tone in CTCSS_TONES if tone != selected)
        nearby = max(max(0.0, self._energy(values, selected - .5)), max(0.0, self._energy(values, selected + .5)))
        total = sum(float(x)*float(x) for x in values)
        return target >= .00002 * len(values) and target >= alternatives * 1.10 and target >= nearby * 1.004 and target >= total * .18
    def process(self, values):
        self.pending.extend(float(x) for x in values)
        while len(self.pending) >= CTCSS_WINDOW:
            present = self._present(self.pending[:CTCSS_WINDOW]); del self.pending[:CTCSS_STEP]
            if present:
                self.good += 1; self.misses = 0
                if self.good >= CTCSS_ATTACK: self.open = True
            else:
                self.good = 0; self.misses += 1
                if self.misses >= CTCSS_RELEASE: self.open = False
        return self.open


def listener_high_pass_taps():
    """Deterministic 48 kHz listener filter; no diagnostic/CTCSS branch exists."""
    midpoint = (LISTENER_HPF_TAPS - 1) / 2
    cutoff = LISTENER_HPF_CUTOFF_HZ / SAMPLE_RATE
    taps = []
    for index in range(LISTENER_HPF_TAPS):
        offset = index - midpoint
        low = 2 * cutoff if offset == 0 else math.sin(2 * math.pi * cutoff * offset) / (math.pi * offset)
        blackman = .42 - .5 * math.cos(2 * math.pi * index / (LISTENER_HPF_TAPS - 1)) + .08 * math.cos(4 * math.pi * index / (LISTENER_HPF_TAPS - 1))
        taps.append((1.0 if offset == 0 else 0.0 - low) * blackman)
    # Spectral inversion must subtract the low pass at the centre as well.
    taps[int(midpoint)] = 1.0 - 2 * cutoff
    return tuple(taps)


def load_profile(path):
    """Read only the minimal approved profile fields; never echo its contents."""
    if not os.path.isabs(path) or not os.path.isfile(path):
        raise ValueError("v2 fixed profile must be an existing absolute task-owned file")
    try:
        with open(path, encoding="utf-8") as profile_file:
            profile = json.load(profile_file)
        frequency = int(profile["frequency_hz"])
        bandwidth = int(profile["channel_bandwidth_hz"])
        gain = profile.get("gain_db")
        if frequency <= 0 or bandwidth != CHANNEL_BANDWIDTH or (gain is not None and not isinstance(gain, (int, float))):
            raise ValueError
        return {"frequency_hz": frequency, "gain_db": gain}
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        raise ValueError("v2 approved fixed profile is invalid") from None


def parse_control(line, threshold, ctcss_tenths):
    fields = line.strip().split()
    if len(fields) != 4 or fields[0] != "set_control": return threshold, ctcss_tenths, None
    try:
        request_id = int(fields[1])
        value, selected = int(fields[2]), int(fields[3])
    except ValueError:
        return threshold, ctcss_tenths, None
    if request_id <= 0 or not MIN_THRESHOLD_TENTHS <= value <= MAX_THRESHOLD_TENTHS or not valid_ctcss_tenths(selected): return threshold, ctcss_tenths, None
    return value, selected, (request_id, value, selected)


def write_ack(control, request, mode):
    """Write only the bounded, exact acknowledgement grammar to the dedicated control fd."""
    if request is None:
        control.write("rejected\n")
    elif mode == "malformed":
        control.write("not-an-ack\n")
    elif mode == "mismatch":
        control.write(f"applied {request[0]} {request[1] - 1} {request[2]}\n")
    elif mode == "out-of-order":
        control.write(f"applied {request[0] + 1} {request[1]} {request[2]}\n")
    else:
        control.write(f"applied {request[0]} {request[1]} {request[2]}\n")
        if mode == "duplicate":
            control.write(f"applied {request[0]} {request[1]} {request[2]}\n")
    control.flush()


def write_ready(control, mode):
    """The one private startup acknowledgement, separate from all controls."""
    if mode == "malformed":
        control.write("ready-not-v2\n")
    elif mode == "control":
        # A control-looking line must never establish child readiness.
        control.write("applied 10001 -1000 1622\n")
    else:
        control.write(READINESS_MARKER + "\n")
        if mode == "duplicate":
            control.write(READINESS_MARKER + "\n")
    control.flush()


class ReceiveGraph:
    """One live flowgraph. ``set_threshold`` never replaces any block."""
    def __init__(self, profile, modules, stdout_fd):
        analog, blocks, filter_, gr, osmosdr = modules
        self.top_block = gr.top_block()
        self.source = osmosdr.source(args="numchan=1")
        self.source.set_sample_rate(SOURCE_RATE)
        self.source.set_center_freq(profile["frequency_hz"], 0)
        if profile["gain_db"] is None:
            self.source.set_gain_mode(True, 0)
        else:
            self.source.set_gain(float(profile["gain_db"]), 0)
        self.channel = filter_.freq_xlating_fir_filter_ccc(
            SOURCE_RATE // SAMPLE_RATE,
            filter_.firdes.low_pass(1.0, SOURCE_RATE, CHANNEL_BANDWIDTH / 2, 2500), 0, SOURCE_RATE)
        # gate=False maintains a continuous PCM clock, with silence when closed.
        self.squelch = analog.pwr_squelch_cc(INITIAL_POWER_SQUELCH_TENTHS / 10.0, 0.0001, 0, False)
        self.demod = analog.nbfm_rx(audio_rate=SAMPLE_RATE, quad_rate=SAMPLE_RATE, tau=75e-6, max_dev=2500)
        self.ctcss = CtcssAudioGate(gr, DEFAULT_CTCSS_TENTHS)
        self.high_pass = filter_.fir_filter_fff(1, listener_high_pass_taps())
        self.to_s16le = blocks.float_to_short(1, 32767)
        self.sink = blocks.file_descriptor_sink(gr.sizeof_short, stdout_fd)
        self.top_block.connect(self.source, self.channel, self.squelch, self.demod, self.ctcss.block, self.high_pass, self.to_s16le, self.sink)
        self.threshold = INITIAL_POWER_SQUELCH_TENTHS

    def set_threshold(self, proposed):
        if not MIN_THRESHOLD_TENTHS <= proposed <= MAX_THRESHOLD_TENTHS:
            return False
        self.squelch.set_threshold(proposed / 10.0)
        self.threshold = proposed
        return True
    def set_control(self, threshold, ctcss_tenths): return self.set_threshold(threshold) and self.ctcss.detector.set_selection(ctcss_tenths)

    def start(self): self.top_block.start()
    def stop(self):
        self.top_block.stop()
        self.top_block.wait()


def runtime_modules():
    try:
        from gnuradio import analog, blocks, filter, gr
        import osmosdr
        return analog, blocks, filter, gr, osmosdr
    except ImportError:
        raise RuntimeError("v2 GNU Radio/osmosdr runtime is unavailable") from None


class CtcssAudioGate:
    def __init__(self, gr, selected_tenths):
        detector = CtcssGate(selected_tenths)
        class Block(gr.sync_block):
            def __init__(self): super().__init__(name="v2_ctcss_audio_gate", in_sig=[numpy.float32], out_sig=[numpy.float32])
            def work(self, input_items, output_items):
                values, output = input_items[0], output_items[0]
                # Scheduler work sizes are arbitrary.  Decide only on bounded
                # packet steps, and apply the state that existed before each
                # step so an opening decision never leaks older buffered audio.
                for start in range(0, len(output), CTCSS_STEP):
                    end = min(start + CTCSS_STEP, len(output)); was_open = detector.open
                    detector.process(values[start:end]); output[start:end] = values[start:end] if was_open else 0.0
                return len(output)
        self.detector, self.block = detector, Block()


def run_synthetic(threshold, ctcss_tenths, control, mode, diagnostics, burst_ms):
    packet = b"\x00" * PCM_BYTES
    if diagnostics:
        sys.stderr.write("gr-osmosdr: using synthetic RTL-SDR diagnostic\n"); sys.stderr.flush()
    if burst_ms:
        # A pipe-sized write may block while its receiver deliberately releases
        # media at 20 ms intervals.  Keep stdin/control servicing independent
        # so that a truthful burst can never delay a live squelch acknowledgement.
        stopped = threading.Event()
        def write_bursts():
            while not stopped.is_set():
                sys.stdout.buffer.write(packet * (burst_ms // 20))
                sys.stdout.buffer.flush()
                stopped.wait(burst_ms / 1000)
        threading.Thread(target=write_bursts, daemon=True).start()
        while True:
            ready, _, _ = select.select([sys.stdin], [], [], 0.02)
            if not ready:
                continue
            line = sys.stdin.readline()
            if not line:
                stopped.set()
                return
            threshold, ctcss_tenths, request = parse_control(line, threshold, ctcss_tenths)
            if diagnostics:
                sys.stderr.write("gr-osmosdr: runtime diagnostic around control\n"); sys.stderr.flush()
            if mode == "timeout":
                time.sleep(.6)
            if mode == "eof":
                stopped.set()
                return
            write_ack(control, request, mode)

    while True:
        ready, _, _ = select.select([sys.stdin], [], [], 0.02)
        if ready:
            line = sys.stdin.readline()
            if not line:
                return
            threshold, ctcss_tenths, request = parse_control(line, threshold, ctcss_tenths)
            if diagnostics:
                sys.stderr.write("gr-osmosdr: runtime diagnostic around control\n"); sys.stderr.flush()
            if mode == "timeout":
                import time
                time.sleep(.6)
            if mode == "eof":
                return
            write_ack(control, request, mode)
        sys.stdout.buffer.write(packet); sys.stdout.buffer.flush()


def wait_without_audio():
    """Synthetic readiness-failure seam; exits cleanly when its owner closes stdin."""
    while sys.stdin.readline():
        pass


def run_production(profile, control):
    graph = ReceiveGraph(profile, runtime_modules(), sys.stdout.fileno())
    graph.start()
    # This is deliberately independent of PCM, carrier, CTCSS and stderr.
    # The receiver will consume it before submitting the initial control.
    write_ready(control, "ready")
    try:
        while True:
            ready, _, _ = select.select([sys.stdin], [], [], 0.1)
            if not ready:
                continue
            line = sys.stdin.readline()
            if not line:
                return
            proposed, selected, request = parse_control(line, graph.threshold, graph.ctcss.detector.selected_tenths)
            if request is not None and not graph.set_control(proposed, selected):
                request = None
            write_ack(control, request, "valid")
    finally:
        graph.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--v2-fixed-profile", required=True)
    parser.add_argument("--power-squelch-tenths", type=int, default=INITIAL_POWER_SQUELCH_TENTHS)
    parser.add_argument("--pcm-stdout", action="store_true")
    parser.add_argument("--control-fd", required=True, type=int)
    parser.add_argument("--synthetic-test", action="store_true")
    parser.add_argument("--synthetic-control-mode", choices=("valid", "malformed", "mismatch", "timeout", "eof", "duplicate", "out-of-order"), default="valid")
    parser.add_argument("--synthetic-readiness-mode", choices=("ready", "delayed", "malformed", "control", "duplicate", "timeout", "eof", "late"), default="ready")
    parser.add_argument("--synthetic-diagnostics", action="store_true")
    parser.add_argument("--synthetic-burst-ms", type=int, default=0)
    args = parser.parse_args()
    if args.power_squelch_tenths != INITIAL_POWER_SQUELCH_TENTHS:
        raise SystemExit("v2 startup threshold must be -100 dB")
    if args.synthetic_test:
        # Synthetic transport proves only the owner/session contract. It never
        # opens or interprets the opaque production profile.
        if not os.path.isabs(args.v2_fixed_profile) or not os.path.isfile(args.v2_fixed_profile):
            raise SystemExit("v2 fixed profile must be an existing absolute task-owned file")
        with os.fdopen(args.control_fd, "w", buffering=1, encoding="utf-8", closefd=True) as control:
            if args.synthetic_burst_ms and not 500 <= args.synthetic_burst_ms <= 550:
                raise SystemExit("v2 synthetic burst must be 500-550 ms")
            if args.synthetic_readiness_mode == "eof":
                return
            if args.synthetic_readiness_mode == "timeout":
                wait_without_audio()
                return
            if args.synthetic_readiness_mode == "late":
                # Deliberately beyond the owner's bounded production deadline.
                time.sleep(11)
            elif args.synthetic_readiness_mode == "delayed":
                # Proves that readiness is not inferred from a sub-second PCM gate.
                time.sleep(1.25)
            write_ready(control, args.synthetic_readiness_mode)
            run_synthetic(args.power_squelch_tenths, DEFAULT_CTCSS_TENTHS, control, args.synthetic_control_mode, args.synthetic_diagnostics, args.synthetic_burst_ms)
        return
    try:
        profile = load_profile(args.v2_fixed_profile)
    except ValueError as error:
        raise SystemExit(str(error)) from None
    try:
        with os.fdopen(args.control_fd, "w", buffering=1, encoding="utf-8", closefd=True) as control:
            run_production(profile, control)
    except RuntimeError as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    main()
