#!/usr/bin/env python3
"""Hardware-free preflight and control coverage for the v2 adapter."""
import importlib.util
import json
import math
from pathlib import Path
import tempfile
import unittest

PATH = Path(__file__).with_name("t28_v2_receive.py")
SPEC = importlib.util.spec_from_file_location("t28_v2_receive", PATH)
ADAPTER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(ADAPTER)


class _Block:
    def __init__(self, *args, **kwargs): self.args = args; self.kwargs = kwargs; self.thresholds = []
    def set_sample_rate(self, value): self.sample_rate = value
    def set_center_freq(self, value, channel): self.frequency = value
    def set_gain_mode(self, value, channel): self.auto_gain = value
    def set_gain(self, value, channel): self.gain = value
    def set_threshold(self, value): self.thresholds.append(value)


class _Top:
    def __init__(self): self.connections = []; self.started = False; self.stopped = False
    def connect(self, *items): self.connections.append(items)
    def start(self): self.started = True
    def stop(self): self.stopped = True
    def wait(self): pass


class V2ReceiveTests(unittest.TestCase):
    def profile(self):
        temp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump({"frequency_hz": 146_000_000, "channel_bandwidth_hz": 12_500}, temp); temp.close()
        self.addCleanup(lambda: Path(temp.name).unlink(missing_ok=True)); return ADAPTER.load_profile(temp.name)

    def modules(self):
        top = _Top()
        class Analog: pwr_squelch_cc = _Block; nbfm_rx = _Block
        class Blocks: float_to_short = _Block; file_descriptor_sink = _Block
        class Firdes:
            @staticmethod
            def low_pass(*args): return ("low", args)
            @staticmethod
            def high_pass(*args): return ("high", args)
        class Filter: freq_xlating_fir_filter_ccc = _Block; fir_filter_fff = _Block; firdes = Firdes
        class Gr:
            sizeof_short = 2
            class sync_block:
                def __init__(self, *args, **kwargs): pass
            @staticmethod
            def top_block(): return top
        class Source(_Block): pass
        class Osmosdr:
            @staticmethod
            def source(**kwargs): return Source(kwargs)
        return (Analog, Blocks, Filter, Gr, Osmosdr), top

    def test_graph_order_rates_and_in_place_threshold(self):
        modules, top = self.modules(); graph = ADAPTER.ReceiveGraph(self.profile(), modules, 1)
        self.assertEqual(ADAPTER.dsp_order(), ("rtl-sdr-source", "channel-12500", "manual-power-squelch", "nbfm", "ctcss-detector-gate-pre-high-pass", "listener-high-pass", "pcm-s16le-48000"))
        self.assertEqual(graph.source.sample_rate, 240000); self.assertEqual(graph.threshold, -1000)
        self.assertEqual(graph.ctcss.detector.selected_tenths, 1622)
        identity = id(graph.squelch); self.assertTrue(graph.set_threshold(-200)); self.assertEqual(id(graph.squelch), identity)
        self.assertEqual(graph.squelch.thresholds, [-20.0]); self.assertFalse(graph.set_threshold(1)); self.assertEqual(graph.threshold, -200)
        graph.start(); graph.stop(); self.assertTrue(top.started and top.stopped)

    def test_control_and_profile_fail_closed(self):
        self.assertEqual(ADAPTER.parse_control("set_control 7 -170 1622", -200, 1230), (-170, 1622, (7, -170, 1622)))
        self.assertEqual(ADAPTER.parse_control("set_control nope", -170, 1622), (-170, 1622, None))
        with self.assertRaises(ValueError): ADAPTER.load_profile("relative.json")
        with self.assertRaises(ValueError): ADAPTER.load_profile(__file__)

    def test_listener_high_pass_rejects_subaudible_energy(self):
        taps = ADAPTER.listener_high_pass_taps()
        def gain(frequency):
            return abs(sum(tap * complex(math.cos(-2 * math.pi * frequency * index / ADAPTER.SAMPLE_RATE), math.sin(-2 * math.pi * frequency * index / ADAPTER.SAMPLE_RATE)) for index, tap in enumerate(taps)))
        attenuation = 20 * math.log10(gain(67) / gain(1000))
        self.assertLess(attenuation, -24)

    def test_manual_ctcss_is_phase_insensitive_selective_and_bounded(self):
        def wave(frequency, start=0, seconds=.02, amplitude=.08, phase=0, voice=False):
            return [amplitude * math.sin(2 * math.pi * frequency * index / ADAPTER.SAMPLE_RATE + phase) + (.025 * math.sin(2 * math.pi * 700 * index / ADAPTER.SAMPLE_RATE) if voice else 0) for index in range(start, start + int(seconds * ADAPTER.SAMPLE_RATE))]
        for amplitude, phase in ((.02, 0), (.2, .71)):
            gate = ADAPTER.CtcssGate(1622)
            for offset in range(0, 8 * ADAPTER.CTCSS_STEP, ADAPTER.CTCSS_STEP): gate.process(wave(162.2, offset, amplitude=amplitude, phase=phase, voice=True))
            self.assertTrue(gate.open)
        for frequency in (159.8, 165.5, 123.0, 50.0, 60.0, 161.2):
            gate = ADAPTER.CtcssGate(1622)
            for offset in range(0, 14 * ADAPTER.CTCSS_STEP, ADAPTER.CTCSS_STEP): gate.process(wave(frequency, offset, amplitude=.12))
            self.assertFalse(gate.open, frequency)
        gate = ADAPTER.CtcssGate(1622)
        for offset in range(0, 8 * ADAPTER.CTCSS_STEP, ADAPTER.CTCSS_STEP): gate.process(wave(162.2, offset, voice=True))
        self.assertTrue(gate.open); gate.process([0.0] * ADAPTER.CTCSS_STEP); self.assertTrue(gate.open)
        for _ in range(5): gate.process([0.0] * ADAPTER.CTCSS_STEP)
        self.assertFalse(gate.open)
        self.assertTrue(ADAPTER.CtcssGate(ADAPTER.NONE_CTCSS_TENTHS).process([0.0] * ADAPTER.CTCSS_STEP))

    def test_real_gnuradio_vector_flowgraph_gates_before_listener_high_pass(self):
        from gnuradio import blocks, filter, gr
        samples = tuple(.08 * math.sin(2 * math.pi * 162.2 * index / ADAPTER.SAMPLE_RATE) + .03 * math.sin(2 * math.pi * 1000 * index / ADAPTER.SAMPLE_RATE) for index in range(10 * ADAPTER.CTCSS_STEP))
        graph = gr.top_block(); source = blocks.vector_source_f(samples); gate = ADAPTER.CtcssAudioGate(gr, 1622).block
        high_pass = filter.fir_filter_fff(1, ADAPTER.listener_high_pass_taps()); sink = blocks.vector_sink_f()
        graph.connect(source, gate, high_pass, sink); graph.run(); output = sink.data()
        self.assertTrue(all(abs(value) < 1e-6 for value in output[:2 * ADAPTER.CTCSS_STEP]))
        self.assertGreater(max(abs(value) for value in output[5 * ADAPTER.CTCSS_STEP:]), .005)


if __name__ == "__main__":
    unittest.main()
