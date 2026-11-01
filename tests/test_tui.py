"""Terminal charts, input, and restoration regressions."""

import base64
import fcntl
import json
import os
from pathlib import Path
import pty
import re
import select
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import time
import unittest
from unittest.mock import Mock, patch
import zipfile
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobmon"))

import live
import plot
import tui
from test_live import events
from test_plot import JOB_ID, RUN_ID, sample

BLUE = tui.rgb("#7aa2f7")
SGR = re.compile(r"\x1b\[[0-9;]*m")
APC = re.compile(rb"\x1b_G([^;\x1b]*)(?:;([^\x1b]*))?\x1b\\")


def point(second, cpu=50.0, memory=4.0, gap=False, final=False):
    return {"time": second * 1000.0, "cpu": cpu, "average": cpu, "maximum": cpu, "memory": memory,
            "memory_peak": memory, "gap": gap, "final": final, "stream_id": "node1--a", "interval": 30,
            "scope": "process_tree_pgid", "samples": 1}


def column_colors(canvas, x):
    starts = ((y * canvas.width + x) * 3 for y in range(canvas.height))
    return {bytes(canvas.pixels[start:start + 3]) for start in starts}


def has_color(canvas, color):
    target = bytes(color)
    pixels = canvas.pixels
    return any(pixels[index:index + 3] == target for index in range(0, len(pixels), 3))


def visible(line):
    return SGR.sub("", line)


class RasterTests(unittest.TestCase):
    def test_scale_follows_the_page(self):
        self.assertEqual(tui.chart_scale("cpu", [point(0, 50)]), 100)
        self.assertEqual(tui.chart_scale("cpu", [point(0, 100)]), 100)
        self.assertAlmostEqual(tui.chart_scale("cpu", [point(0, 108)]), 118.8)
        self.assertAlmostEqual(tui.chart_scale("memory", [point(0, memory=10)]), 11)
        self.assertEqual(tui.chart_scale("memory", [dict(point(0, memory=None), memory_peak=None)]), 1)

    def test_an_outage_stays_blank_and_is_never_drawn_as_zero(self):
        points = [point(second) for second in range(0, 1000, 10)]
        points += [point(2000, gap=True)] + [point(second) for second in range(2010, 3000, 10)]
        canvas, x_of = tui.render_chart("cpu", points, 600, 200, 1.0)
        background = {bytes(tui.BACKGROUND), bytes(tui.GRID), bytes(tui.REFERENCE)}
        for second in (1300, 1500, 1800):
            self.assertLessEqual(column_colors(canvas, int(x_of(second * 1000))), background, "a gap was drawn on")
        self.assertGreater(column_colors(canvas, int(x_of(500 * 1000))) - background, set())

    def test_a_missing_reading_lifts_the_pen_without_a_zero(self):
        points = [point(second) for second in range(0, 100, 10)]
        points.append(dict(point(100), cpu=None, average=None, maximum=None))
        points += [point(second) for second in range(110, 200, 10)]
        runs = tui.polylines(points, "cpu", float, lambda value: value)
        self.assertEqual(len(runs), 2)
        self.assertTrue(all(y == 50 for run in runs for _, y in run))

    def test_a_one_sample_spike_survives_a_week_of_history(self):
        points = [point(second * 60, 10) for second in range(20000)]
        points[12345] = point(12345 * 60, 300)
        points[12345]["average"] = points[12345]["maximum"] = 10
        canvas, _ = tui.render_chart("cpu", points, 800, 200, 1.0)
        upper = tui.Canvas.__new__(tui.Canvas)
        upper.pixels = canvas.pixels[:60 * canvas.width * 3]
        self.assertTrue(has_color(upper, BLUE), "the spike vanished into its pixel column")

    def test_drawing_work_follows_width_not_sample_count(self):
        points = [point(second, (second * 7919) % 100) for second in range(50000)]
        width = 800
        runs = tui.polylines(points, "cpu", lambda when: when / 50000000 * width, lambda value: value)
        self.assertLessEqual(sum(len(run) for run in runs), 4 * (width + 1))

    def test_lone_readings_and_edges_draw_inside_the_image(self):
        canvas, _ = tui.render_chart("cpu", [point(0, 0, gap=True)], 300, 100, 2.0)
        self.assertTrue(has_color(canvas, tui.rgb("#9ece6a")), "the last series drawn should mark the lone reading")
        for values in ((0, 100), (100, 0), (1000, 0.001)):
            points = [point(0, values[0]), point(30, values[1])]
            tui.render_chart("cpu", points, 120, 60, 4.0)

    def test_text_chart_keeps_the_same_gaps_and_spikes(self):
        points = [point(second, 50) for second in range(0, 300, 30)] + [point(2000, 50, gap=True)]
        points += [point(second, 50) for second in range(2030, 2300, 30)]
        rows = tui.text_chart("cpu", points, 60, 8)
        strips = [visible(row) for row in rows]
        self.assertTrue(all(len(strip) == 60 for strip in strips))
        drawn = {char for strip in strips[1:] for char in strip[25:40]}
        self.assertLessEqual(drawn, {" "}, "the outage was drawn on")


class ProtocolTests(unittest.TestCase):
    def decode(self, commands):
        found = APC.findall(commands)
        self.assertEqual(b"".join(re.findall(rb"\x1b_G.*?\x1b\\", commands, re.S)), commands)
        return found

    def test_image_commands_follow_the_chunking_rules(self):
        canvas = tui.Canvas(300, 200, tui.BACKGROUND)
        canvas.pixels[:] = os.urandom(len(canvas.pixels))  # Incompressible, so it must span chunks.
        found = self.decode(tui.image_commands(7301, canvas, 40, 10))
        self.assertGreater(len(found), 2)
        keys = dict(item.split(b"=") for item in found[0][0].split(b","))
        self.assertEqual(keys, {b"a": b"T", b"f": b"24", b"o": b"z", b"s": b"300", b"v": b"200", b"i": b"7301",
                                b"p": b"1", b"c": b"40", b"r": b"10", b"C": b"1", b"q": b"2", b"m": b"1"})
        for control, _ in found[1:-1]:
            self.assertEqual(control, b"m=1,q=2")
        self.assertEqual(found[-1][0], b"m=0,q=2")
        self.assertTrue(all(0 < len(chunk) <= 4096 and len(chunk) % 4 == 0 for _, chunk in found[:-1]))
        decoded = zlib.decompress(base64.standard_b64decode(b"".join(chunk for _, chunk in found)))
        self.assertEqual(decoded, bytes(canvas.pixels))

    def test_a_small_image_is_one_final_chunk(self):
        found = self.decode(tui.image_commands(7302, tui.Canvas(40, 20, tui.BACKGROUND), 4, 1))
        self.assertEqual(len(found), 1)
        self.assertIn(b"m=0", found[0][0])

    def present(self, terminal, frame):
        with tempfile.TemporaryFile() as stream:
            terminal.output = stream.fileno()
            terminal.present(frame)
            stream.seek(0)
            return stream.read()

    def placement(self, points, cursor=None, signature="a"):
        return tui.Placement("cpu", points, 2, 8, 40, 6, (10, 20), cursor, signature, (0.0, 30000.0), 100)

    def test_present_sends_only_what_changed(self):
        points = [point(0), point(30)]
        terminal = tui.Terminal(0, 1)
        lines = ["title", "legend", "  100% ", "", "", "", "", ""]
        frame = tui.Frame(lines, [self.placement(points)])
        first = self.present(terminal, frame)
        self.assertIn(b"title\x1b[K", first)
        self.assertIn(b"\x1b[3;9H\x1b_G", first)
        self.assertNotIn("  100% \x1b[K".encode(), first)
        self.assertEqual(self.present(terminal, frame), b"\x1b[?2026h\x1b[?2026l")
        moved = self.present(terminal, tui.Frame(lines, [self.placement(points, cursor=15000.0)]))
        self.assertIn(b"a=T", moved)
        self.assertNotIn(b"title", moved)
        removed = self.present(terminal, tui.Frame(lines, []))
        self.assertIn(b"a=d,d=I,i=7301", removed)
        self.assertIn(b"\x1b[3;1H  100% \x1b[K", removed)

    def test_cursor_moves_reuse_the_drawn_chart(self):
        points = [point(0), point(30)]
        terminal = tui.Terminal(0, 1)
        with patch.object(tui, "render_chart", wraps=tui.render_chart) as render:
            self.present(terminal, tui.Frame([""] * 8, [self.placement(points)]))
            self.present(terminal, tui.Frame([""] * 8, [self.placement(points, cursor=15000.0)]))
            self.assertEqual(render.call_count, 1)
            self.present(terminal, tui.Frame([""] * 8, [self.placement(points, signature="b")]))
            self.assertEqual(render.call_count, 2)


class DetectionTests(unittest.TestCase):
    def detect(self, reply, suffix=b""):
        read_in, write_in = os.pipe()
        with tempfile.TemporaryFile() as sink:
            os.write(write_in, reply)
            terminal = tui.Terminal(read_in, sink.fileno())
            with patch.object(tui, "DETECT_SECONDS", 0.05):
                result = terminal.detect_graphics()
            if suffix:
                supported, typed = result
                result = supported, typed + terminal.decoder.feed(suffix)
            sink.seek(0)
            sent = sink.read()
        os.close(read_in)
        os.close(write_in)
        return result, sent

    def test_support_needs_the_terminals_own_ok_before_its_device_attributes(self):
        for reply, supported in ((b"\x1b_Gi=31;OK\x1b\\\x1b[?62;22;52c", True),
                                 (b"\x1b[?62;22c", False),
                                 (b"\x1b_Gi=31;ENOTSUPPORTED:no\x1b\\\x1b[?62c", False),
                                 (b"", False)):
            with self.subTest(reply=reply):
                (found, _), sent = self.detect(reply)
                self.assertEqual(found, supported)
                self.assertTrue(sent.startswith(tui.GRAPHICS_QUERY))
                self.assertIn(b"\x1b[2J", sent)

    def test_keys_typed_during_the_query_are_kept(self):
        (found, typed), _ = self.detect(b"q\x1b_Gi=31;OK\x1b\\\x1b[?62c")
        self.assertTrue(found)
        self.assertEqual(typed, ["q"])

    def test_a_key_started_during_detection_finishes_in_the_same_decoder(self):
        (_, typed), _ = self.detect(b"\x1b[?62c\x1b[", suffix=b"H")
        self.assertEqual(typed, ["home"])


class KeyTests(unittest.TestCase):
    def test_keys_replies_and_unknown_sequences(self):
        decode = tui.KeyDecoder().feed
        self.assertEqual(decode(b"q\thlHL\x1b[A\x1bOB\x1b[Z\x03"),
                         ["q", "tab", "h", "l", "H", "L", "up", "down", "backtab", "ctrl-c"])
        self.assertEqual(decode(b"\x08\x0c"), ["ctrl-h", "ctrl-l"])
        self.assertEqual(decode(b"\x1b[H\x1b[F"), ["home", "end"])
        self.assertEqual(decode(b"\x1b[C\x1b[D\x1bOC\x1bOD\x1b[1;2C\x1b[1;2D"), [])
        self.assertEqual(decode(b"\x1b[?62;22c\x1b_Gi=7301;OK\x1b\\\x1b[15~a"), ["a"])
        self.assertEqual(decode("한".encode()), [])

    def test_escape_waits_briefly_for_the_rest_of_a_sequence(self):
        decoder = tui.KeyDecoder()
        self.assertEqual(decoder.feed(b"\x1b", now=1), [])
        with patch.object(tui.time, "monotonic", return_value=1):
            self.assertAlmostEqual(decoder.wait_seconds(), tui.ESCAPE_SECONDS)
        self.assertEqual(decoder.feed(now=1 + tui.ESCAPE_SECONDS / 2), [])
        self.assertEqual(decoder.feed(now=1 + tui.ESCAPE_SECONDS), ["esc"])
        self.assertEqual(decoder.feed(now=2), [])
        self.assertEqual(decoder.feed(b"h", now=2), ["h"])

    def test_every_key_and_reply_can_be_split_at_any_byte(self):
        sequences = [(sequence, [key]) for sequence, key in tui.KEY_SEQUENCES.items()]
        sequences += [(b"\x1b[1;2C", []), (b"\x1b[?62;22c", []), (b"\x1b_Gi=7301;OK\x1b\\", [])]
        for sequence, expected in sequences:
            for split in range(1, len(sequence)):
                with self.subTest(sequence=sequence, split=split):
                    decoder = tui.KeyDecoder()
                    self.assertEqual(decoder.feed(sequence[:split], now=1), [])
                    self.assertEqual(decoder.feed(sequence[split:], now=1.001), expected)
                    self.assertEqual(decoder.feed(now=2), [])

    def test_fragmented_reply_payload_never_becomes_commands(self):
        decoder = tui.KeyDecoder()
        for chunk in (b"\x1b_", b"Gi=7301;ENOENT:", b"qhl" * 10000, b"\x1b"):
            self.assertEqual(decoder.feed(chunk), [])
        self.assertEqual(decoder.feed(b"\\s"), ["s"])

    def test_an_interrupted_sequence_still_allows_ctrl_c(self):
        for prefix in (b"\x1b[", b"\x1bO"):
            decoder = tui.KeyDecoder()
            self.assertEqual(decoder.feed(prefix), [])
            self.assertEqual(decoder.feed(b"\x03"), ["ctrl-c"])


class InputLoopTests(unittest.TestCase):
    def view(self):
        state = live.MultiJobState([JOB_ID, "456_2"], 5)
        for job in state.jobs:
            for event in events(job_id=job.job_id):
                job.apply(event)
        view = tui.Dashboard(False)
        view.update(state.view(JOB_ID))
        return state, view

    def loop(self, state, view, chunks, initial_keys=()):
        terminal = Mock(input=0, decoder=tui.KeyDecoder())
        terminal.size.return_value = (100, 30, (10, 20))
        with patch.object(tui.select, "select", return_value=([0], [], [])), \
                patch.object(tui.os, "read", side_effect=chunks):
            self.assertEqual(tui.view_loop(state, terminal, view, [False], initial_keys=initial_keys), 0)

    def test_batched_job_switches_save_the_newly_selected_job(self):
        state, view = self.view()
        with patch.object(tui, "write_exports", return_value=[]) as export:
            self.loop(state, view, [b"\ts\x1b[Zsq"])
        self.assertEqual([call.args[0]["job_id"] for call in export.call_args_list], ["456_2", JOB_ID])

    def test_keys_typed_during_detection_use_the_loaded_selection(self):
        state, view = self.view()
        with patch.object(tui, "write_exports", return_value=[]) as export:
            self.loop(state, view, [], initial_keys=["tab", "s", "q"])
        self.assertEqual(export.call_args.args[0]["job_id"], "456_2")

    def test_batched_switch_then_run_navigation_uses_the_new_job(self):
        state, view = self.view()
        job = state.jobs[1]
        metadata = dict(job.runs[RUN_ID]["metadata"], run_id="20260902T000000Z-r0-abcdef012346")
        job.apply({"event": "run", "schema_version": 1, "job_id": job.job_id, "metadata": metadata})
        self.loop(state, view, [b"\tRq"])
        self.assertEqual(view.views["456_2"]["run"], RUN_ID)
        self.assertNotIn(JOB_ID, view.views)

    def test_split_home_is_navigation_instead_of_escape_and_inspect(self):
        state, view = self.view()
        view.key("end")
        self.loop(state, view, [b"\x1b[", b"H", b"q"])
        self.assertEqual(view.cursor, view.selection()[2]["points"][0]["time"])


class TerminalRestorationTests(unittest.TestCase):
    def test_screen_output_failures_always_restore_terminal_settings(self):
        for failure in ("enter", "leave", "both"):
            with self.subTest(failure=failure):
                master, slave = pty.openpty()
                before = termios.tcgetattr(slave)
                terminal = tui.Terminal(slave, slave)
                error = OSError("synthetic screen output failure")
                writes = [error, None] if failure == "enter" else [None, error] if failure == "leave" else error
                try:
                    with patch.object(tui, "Terminal", return_value=terminal), \
                            patch.object(terminal, "write", side_effect=writes), \
                            patch.object(tui, "choose_charts", return_value=(False, None, [])), \
                            patch.object(tui, "view_loop", return_value=0):
                        with self.assertRaisesRegex(OSError, "synthetic screen output failure"):
                            tui.run(None, text_only=True)
                    self.assertEqual(termios.tcgetattr(slave), before)
                finally:
                    termios.tcsetattr(slave, termios.TCSANOW, before)
                    os.close(slave)
                    os.close(master)


class CompletionTests(unittest.TestCase):
    def test_outcomes_need_evidence(self):
        gone = {"state": None, "checked_at": "2026-09-01T01:00:00+00:00"}
        running = {"state": "RUNNING", "checked_at": "2026-09-01T01:00:00+00:00"}
        created = {"metadata": {"created_at": "2026-09-01T00:00:00+00:00"}, "exit_code": None}
        self.assertEqual(plot.completion_result("1", {"exit_code": 0}, None)["kind"], "finished")
        failed = plot.completion_result("1", {"exit_code": 3}, None)
        self.assertEqual((failed["kind"], failed["label"]), ("failed", "exit 3"))
        self.assertIn("exited with code 3", failed["message"])
        self.assertIsNone(plot.completion_result("1", created, None))
        self.assertIsNone(plot.completion_result("1", created, running))
        self.assertIsNone(plot.completion_result("1", created, gone, latest=False))
        self.assertIsNone(plot.completion_result("1", created, {"state": None, "checked_at": "bad"}))
        self.assertEqual(plot.completion_result("1", created, gone)["kind"], "unknown")
        self.assertEqual(plot.completion_result("1", None, gone)["kind"], "unknown")
        late = {"metadata": {"created_at": "2026-09-01T02:00:00+00:00"}, "exit_code": None}
        self.assertIsNone(plot.completion_result("1", late, gone), "a newer run cannot have left the older queue check")
        attached = {"metadata": {"attachment": {"pid": 1}}, "exit_code": None}
        self.assertIn("Attached monitoring", plot.completion_result("1", attached, gone)["detail"])
        self.assertIsNone(plot.completion_result("1", {"exit_code": True}, None), "a boolean is not an exit code")

    def test_python_and_page_tell_the_same_story(self):
        for run, status in (({"exit_code": 0}, None), ({"exit_code": 1}, None),
                            (None, {"state": None, "checked_at": "2026-09-01T01:00:00+00:00"})):
            result = plot.completion_result("1", run, status)
            self.assertIn(result["label"].split()[0], plot.HTML)
            self.assertIn(result["message"].split(" code ")[0][:25], plot.HTML)


class LayoutTests(unittest.TestCase):
    def view(self, rows=None, jobs=(JOB_ID,), pixels=False, **options):
        state = live.MultiJobState(list(jobs), 5)
        for job in state.jobs:
            for event in events(rows, job.job_id):
                job.apply(event)
        view = tui.Dashboard(pixels, **options)
        view.update(state.view(state.jobs[0].job_id))
        return view, state

    def frame(self, view, cols=100, rows=30):
        return view.compose(cols, rows, (10, 20))

    def status_with(self, state, completion):
        job = state.jobs[0]
        job.apply({"event": "status", "job_id": job.job_id, "schema_version": 1, "generated_at": "t", "errors": [],
                   "runs": [{"run_id": RUN_ID, "completion": completion, "errors": []}]})

    def test_every_frame_fills_the_screen_within_its_width(self):
        view, _ = self.view()
        for cols, rows in ((50, 16), (80, 24), (120, 40), (200, 60)):
            for mode in (False, True):
                with self.subTest(size=(cols, rows), pixels=mode):
                    view.pixels = mode
                    frame = view.compose(cols, rows, (10, 20))
                    self.assertEqual(len(frame.lines), rows)
                    for line in frame.lines:
                        self.assertLessEqual(sum(tui.cell_width(char) for char in visible(line)), cols, line)

    def test_small_terminals_say_so_instead_of_drawing_junk(self):
        view, _ = self.view()
        frame = view.compose(40, 10, (10, 20))
        self.assertIn("too small", frame.lines[0])
        self.assertEqual(frame.images, [])

    def test_pixels_place_images_and_text_mode_draws_braille(self):
        view, _ = self.view(pixels=True)
        frame = self.frame(view)
        self.assertEqual([image.chart for image in frame.images], ["cpu", "memory"])
        self.assertFalse(any("\u2800" <= char <= "\u28ff" for line in frame.lines for char in line))
        top = frame.images[0]
        self.assertEqual((top.left, top.cols), (tui.GUTTER, 100 - tui.GUTTER - 1))
        self.assertIn("%", visible(frame.lines[top.top]))
        view.pixels = False
        frame = self.frame(view)
        self.assertEqual(frame.images, [])
        self.assertTrue(any("\u2800" < char <= "\u28ff" for line in frame.lines for char in line))

    def test_the_measurement_scope_is_always_named(self):
        view, _ = self.view()
        self.assertIn("process_tree_pgid", "\n".join(visible(line) for line in self.frame(view).lines))

    def test_a_missing_reading_reads_unavailable_never_zero(self):
        view, _ = self.view(rows=[sample(0)])
        text = "\n".join(visible(line) for line in self.frame(view).lines)
        self.assertIn("Last interval unavailable", text)
        self.assertNotIn("Last interval 0", text)

    def test_waiting_and_connecting_states(self):
        self.assertIn("Connecting", tui.Dashboard(False).compose(80, 24, (10, 20)).lines[0])
        state = live.MultiJobState([JOB_ID], 5)
        view = tui.Dashboard(True)
        view.update(state.view(JOB_ID))
        frame = self.frame(view)
        self.assertEqual(frame.images, [])
        self.assertIn("Waiting for saved samples", "\n".join(frame.lines))
        self.assertIn("Connecting", "\n".join(frame.lines))

    def test_remote_text_cannot_reach_the_terminal_as_control_codes(self):
        hostile = "bad\x1b]0;pwned\x07\x1b[2Jname\n"
        view, state = self.view()
        state.jobs[0]._error(hostile)
        with state.changed:
            run = state.jobs[0].runs[RUN_ID]
            run["job_name"] = hostile
            state.jobs[0].scheduler["queue"][0]["state"] = hostile
        view.update(state.view(JOB_ID))
        view.key("d")
        for line in self.frame(view).lines:
            self.assertEqual(SGR.sub("", line).count("\x1b"), 0, line)
            self.assertNotIn("\x07", line)

    def test_wide_names_are_cut_by_cells(self):
        spans = [("가" * 10, ""), ("x", "")]
        self.assertEqual(visible(tui.fit(spans, 7)), "가" * 3)
        self.assertEqual(visible(tui.fit(spans, 21)), "가" * 10 + "x")

    def test_missing_nodes_survive_a_narrow_window(self):
        view, state = self.view()
        with state.changed:
            state.jobs[0].runs[RUN_ID]["expected_nodes"] = ["node1", "node2"]
        view.update(state.view(JOB_ID))
        self.assertIn("Missing: node2", visible(view.compose(80, 24, (10, 20)).lines[1]))

    def test_outcome_banners_follow_the_evidence(self):
        view, state = self.view()
        text = "\n".join(visible(line) for line in self.frame(view).lines)
        self.assertNotIn("Workload finished", text)
        self.assertNotIn("left the Slurm queue", text)
        state.jobs[0].disconnected("Remote stream ended (SSH exit 255)")
        view.update(state.view(JOB_ID))
        self.assertNotIn("Workload", "\n".join(self.frame(view).lines), "disconnection alone proves nothing")
        state.jobs[0].update_queue(None, "2026-09-01T01:00:00+00:00")
        view.update(state.view(JOB_ID))
        self.assertIn("Job left the Slurm queue", "\n".join(visible(line) for line in self.frame(view).lines))
        self.status_with(state, {"job_id": JOB_ID, "run_id": RUN_ID, "workload_exit_code": 7})
        view.update(state.view(JOB_ID))
        text = "\n".join(visible(line) for line in self.frame(view).lines)
        self.assertIn("Workload exited with code 7", text)
        self.assertIn("Wrapper exit: 7", "\n".join(visible(line) for line in self.frame(view, cols=140).lines))

    def test_tabs_mark_outcomes_and_keep_the_selected_job_visible(self):
        view, state = self.view(jobs=["{}_{}".format(123, index) for index in range(30)])
        self.assertIn("123_0", visible(self.frame(view).lines[0]))
        for _ in range(25):
            view.key("tab")
            view.update(state.view(view.selected))
        self.assertEqual(view.selected, "123_25")
        line = visible(self.frame(view).lines[0])
        self.assertIn("123_25", line)
        self.assertTrue(line.startswith("< "))
        view.key("backtab")
        self.assertTrue(view.stale)
        self.assertEqual(view.selected, "123_24")
        self.status_with(state, {"job_id": "123_0", "run_id": RUN_ID, "workload_exit_code": 0})
        view.selected = "123_0"
        view.update(state.view("123_0"))
        self.assertIn("123_0 · finished", visible(self.frame(view).lines[0]))

    def test_view_copies_one_jobs_history_and_keeps_tabs_light(self):
        state = live.MultiJobState([JOB_ID, "456_2"], 5)
        for job in state.jobs:
            for event in events(job_id=job.job_id):
                job.apply(event)
        result = state.view("456_2")
        self.assertEqual(result["job"]["job_id"], "456_2")
        self.assertEqual(len(result["job"]["runs"][0]["nodes"][0]["points"]), 3)
        self.assertNotIn("points", json.dumps(result["jobs"]))

    def test_cursor_reads_the_nearest_sample_on_both_axes(self):
        view, _ = self.view(rows=[sample(0), sample(30, 60), sample(60, 120)])
        view.key("h")
        self.assertIn("at 0:01:00", "\n".join(visible(line) for line in self.frame(view).lines))
        view.key("h")
        text = "\n".join(visible(line) for line in self.frame(view).lines)
        self.assertIn("at 0:00:30", text)
        self.assertIn("Last interval 50.00%", text)
        view.key("l")
        self.assertIn("at 0:01:00", "\n".join(visible(line) for line in self.frame(view).lines))
        view.key("H")
        view.key("H")
        view.key("H")
        self.assertIn("at 0:00:00", "\n".join(visible(line) for line in self.frame(view).lines))
        view.key("L")
        self.assertIn("at 0:00:30", "\n".join(visible(line) for line in self.frame(view).lines))
        view.key("h")
        with patch.dict(os.environ, {"TZ": "UTC"}):
            time.tzset()
            view.key("a")
            text = "\n".join(visible(line) for line in self.frame(view).lines)
        time.tzset()
        self.assertIn("at 00:00:00", text)
        view.key("esc")
        self.assertIsNone(view.cursor)

    def test_a_placed_image_carries_the_cursor(self):
        view, _ = self.view(pixels=True)
        view.key("home")
        self.assertIsNotNone(self.frame(view).images[0].cursor)

    def test_runs_follow_the_newest_until_an_older_one_is_chosen(self):
        view, state = self.view()
        second = "20260902T000000Z-r0-abcdef012346"
        job = state.jobs[0]
        meta = dict(job.runs[RUN_ID]["metadata"], run_id=second)
        job.apply({"event": "run", "metadata": meta, "job_id": JOB_ID, "schema_version": 1})
        view.update(state.view(JOB_ID))
        self.assertEqual(view.selection()[1]["run_id"], second)
        view.key("R")
        self.assertEqual(view.selection()[1]["run_id"], RUN_ID)
        third = "20260903T000000Z-r0-abcdef012347"
        job.apply({"event": "run", "metadata": dict(meta, run_id=third), "job_id": JOB_ID, "schema_version": 1})
        view.update(state.view(JOB_ID))
        self.assertEqual(view.selection()[1]["run_id"], RUN_ID, "an explicit older run stays selected")
        view.key("r")
        view.key("r")
        self.assertEqual(view.selection()[1]["run_id"], third)
        self.assertIsNone(view.views[JOB_ID]["run"], "landing on the newest run resumes following")

    def test_details_panel_replaces_the_charts_and_scrolls(self):
        view, state = self.view(pixels=True)
        for index in range(60):
            state.jobs[0]._error("problem {}".format(index))
        view.update(state.view(JOB_ID))
        self.assertIn("d details (60)", visible(self.frame(view).lines[-1]))
        view.key("d")
        frame = self.frame(view)
        self.assertEqual(frame.images, [])
        text = "\n".join(visible(line) for line in frame.lines)
        self.assertIn("Diagnostics (60)", text)
        self.assertIn("Scope process_tree_pgid", text)
        view.key("down")
        scrolled = "\n".join(visible(line) for line in self.frame(view).lines)
        self.assertNotEqual(text, scrolled)
        view.key("esc")
        self.assertEqual(len(self.frame(view).images), 2)

    def test_save_writes_the_selected_node_on_the_chosen_axis(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "saved"
            view, _ = self.view(output_dir=str(destination))
            view.key("a")
            view.key("s")
            archives = sorted(destination.glob("*.zip"))
            self.assertEqual(len(archives), 2)
            self.assertEqual(destination.stat().st_mode & 0o777, 0o700)
            with zipfile.ZipFile(str(archives[0])) as bundle:
                self.assertEqual(json.loads(bundle.read("data.json"))["time_axis"], "clock")
                self.assertIn("samples.jsonl", bundle.namelist())
            self.assertIn("Saved 2 ZIPs", visible(self.frame(view).lines[-1]))

    def test_a_failed_save_is_reported_not_raised(self):
        with tempfile.NamedTemporaryFile() as occupied:
            view, _ = self.view(output_dir=occupied.name)
            view.key("s")
            self.assertIn("Save failed", visible(self.frame(view).lines[-1]))
        view, _ = self.view(rows=[])
        view.key("s")
        self.assertIn("No saved samples", view.message[0])

    def test_a_terminal_without_graphics_is_told_not_hidden(self):
        view, _ = self.view(notice="Pixel charts need Kitty graphics")
        self.assertIn("Pixel charts need", visible(self.frame(view).lines[-1]))
        view.message = None
        self.assertNotIn("Pixel charts need", visible(self.frame(view).lines[-1]))
        view.key("d")
        self.assertIn("Pixel charts need", "\n".join(visible(line) for line in self.frame(view).lines))

    def test_quit_keys_choose_the_exit_status(self):
        view, _ = self.view()
        self.assertTrue(view.key("q"))
        self.assertEqual(view.exit_status, 0)
        self.assertTrue(view.key("ctrl-c"))
        self.assertEqual(view.exit_status, 130)
        self.assertFalse(view.key("x"))


HOUR = 3600


def long_points(hours=72, spike_hour=None):
    rows = [point(hour * HOUR, 50) for hour in range(hours + 1)]
    if spike_hour is not None:
        rows[spike_hour].update(cpu=180.0)
    return rows


def job_data(points, job_id=JOB_ID):
    run = {"run_id": RUN_ID, "job_name": "x", "exit_code": None, "metadata": {}, "expected_nodes": ["node1"],
           "expected_node_count": 1, "runtime": {"source": "slurm", "seconds": 1, "display": "0:01"},
           "nodes": [{"node": "node1", "scopes": ["process_tree_pgid"], "points": points}]}
    return {"job_id": job_id, "runs": [run], "diagnostics": [], "scope_notes": {}, "connection": "Live",
            "queue_status": None, "scheduler": None, "scheduler_snapshot_at": None}


def view_of(points, pixels=False):
    job = job_data(points)
    view = tui.Dashboard(pixels)
    view.update({"jobs": [{"job_id": JOB_ID, "connection": "Live", "runs": job["runs"], "queue_status": None}],
                 "job": job, "discovery_error": None})
    return view


class WindowTests(unittest.TestCase):
    def text(self, view, cols=120, rows=40):
        return "\n".join(visible(line) for line in view.compose(cols, rows, (10, 20)).lines)

    def edges(self, view):
        points = view.selection()[2]["points"]
        start, end = tui.chart_window(points, view.window_end)
        return start / 1000 / HOUR, end / 1000 / HOUR

    def test_a_day_or_less_is_drawn_whole(self):
        for hours in (1, 24):
            points = long_points(hours)
            self.assertEqual(tui.chart_window(points), (points[0]["time"], points[-1]["time"]))
            self.assertNotIn("Showing", self.text(view_of(points)))

    def test_longer_histories_open_on_the_newest_day_and_follow_it(self):
        points = long_points()
        view = view_of(points)
        self.assertEqual(self.edges(view), (48, 72))
        text = self.text(view)
        self.assertIn("Showing the newest 24 hours of 72:00:00", text)
        self.assertIn("48:00:00", text)
        points.append(point(73 * HOUR, 50))
        self.assertEqual(self.edges(view), (49, 73), "with no cursor the window follows new samples")

    def test_only_the_window_and_its_neighbours_are_sent_to_the_charts(self):
        points = long_points()
        window = tui.chart_window(points)
        self.assertEqual([row["time"] / 1000 / HOUR for row in tui.window_points(points, window)],
                         list(range(47, 73)))
        placed = view_of(points, pixels=True).compose(120, 40, (10, 20)).images[0]
        self.assertEqual(placed.window, window)
        self.assertEqual(len(placed.points), 26)

    def test_h_and_l_unfold_what_lies_beyond_the_edge(self):
        view = view_of(long_points())
        view.key("h")
        self.assertEqual(view.cursor / 1000 / HOUR, 72, "the first press lands on the newest sample")
        for _ in range(24):
            view.key("h")
        self.assertEqual((view.cursor / 1000 / HOUR, self.edges(view)), (48, (48, 72)))
        view.key("h")
        self.assertEqual((view.cursor / 1000 / HOUR, self.edges(view)), (47, (47, 71)))
        self.assertIn("Showing 47:00:00 to 71:00:00 of 72:00:00", self.text(view))
        for _ in range(24):
            view.key("l")
        self.assertEqual((view.cursor / 1000 / HOUR, self.edges(view)), (71, (47, 71)), "the window holds")
        view.key("l")
        self.assertEqual(self.edges(view), (48, 72))

    def test_ctrl_keys_move_a_quarter_of_the_window_at_a_time(self):
        view = view_of(long_points())
        view.key("ctrl-h")
        self.assertEqual(view.cursor / 1000 / HOUR, 66, "a first press starts from the newest sample")
        view.key("ctrl-h")
        self.assertEqual(view.cursor / 1000 / HOUR, 60)
        view.key("ctrl-l")
        self.assertEqual(view.cursor / 1000 / HOUR, 66)
        for _ in range(20):
            view.key("ctrl-h")
        self.assertEqual((view.cursor, self.edges(view)), (0, (0, 24)), "it stops at the first sample")
        for _ in range(20):
            view.key("ctrl-l")
        self.assertEqual((view.cursor / 1000 / HOUR, self.edges(view)), (72, (48, 72)))

    def test_jumps_scale_with_a_short_history_but_always_move(self):
        minutes = [point(minute * 60, 50) for minute in range(121)]
        view = view_of(minutes)
        view.key("ctrl-h")
        self.assertEqual(view.cursor / 1000 / 60, 90, "a quarter of two hours")
        view.key("H")
        self.assertEqual(view.cursor / 1000 / 60, 84, "5% of two hours")
        few = view_of([point(0), point(30), point(60)])
        few.key("l")
        few.key("H")
        self.assertEqual(few.cursor, 30000, "a short jump still moves one sample")

    def test_inspecting_holds_the_window_while_new_samples_arrive(self):
        points = long_points()
        view = view_of(points)
        view.key("ctrl-h")
        before = self.edges(view)
        points.append(point(73 * HOUR, 50))
        self.assertEqual(self.edges(view), before)
        view.key("esc")
        self.assertEqual((view.cursor, self.edges(view)), (None, (49, 73)), "Esc returns to the newest")

    def test_home_and_end_reach_both_ends(self):
        view = view_of(long_points())
        view.key("home")
        self.assertEqual(self.edges(view), (0, 24))
        view.key("end")
        self.assertEqual(self.edges(view), (48, 72))

    def test_the_scale_comes_from_the_whole_run_so_scrolling_compares_like_with_like(self):
        view = view_of(long_points(spike_hour=3), pixels=True)
        opening = view.compose(120, 40, (10, 20))
        self.assertAlmostEqual(opening.images[0].maximum, 198.0)
        view.key("home")
        scrolled = view.compose(120, 40, (10, 20))
        self.assertEqual(scrolled.images[0].maximum, opening.images[0].maximum)
        labels = [visible(line)[:tui.GUTTER] for line in opening.lines[opening.images[0].top:][:10]]
        self.assertIn("198%", "".join(labels))

    def test_a_spike_only_appears_once_scrolled_into_view(self):
        view = view_of(long_points(spike_hour=3), pixels=True)
        reds = []
        for action in (None, "home"):
            if action:
                view.key(action)
            placed = view.compose(120, 40, (10, 20)).images[0]
            canvas, _ = tui.render_chart("cpu", placed.points, 600, 200, 1.0, placed.window, placed.maximum)
            upper = tui.Canvas.__new__(tui.Canvas)
            upper.pixels = canvas.pixels[:40 * canvas.width * 3]
            reds.append(has_color(upper, BLUE))
        self.assertEqual(reds, [False, True])

    def test_lines_are_clipped_at_the_window_edges_in_both_chart_kinds(self):
        points = long_points()
        window = tui.chart_window(points)
        shown = tui.window_points(points, window)
        tui.render_chart("cpu", shown, 300, 100, 2.0, window, 100)
        rows = tui.text_chart("cpu", shown, 40, 6, None, window, 100)
        self.assertEqual({len(visible(row)) for row in rows}, {40})
        drawn = visible(rows[3])
        self.assertTrue(drawn[0] != " " and drawn[-1] != " ", "the line should reach both edges: " + drawn)

    def test_an_outage_filling_the_window_stays_blank(self):
        points = [point(hour * HOUR, 50) for hour in range(5)]
        points += [point(hour * HOUR, 50, gap=hour == 80) for hour in range(80, 90)]
        window = tui.chart_window(points, 64 * HOUR * 1000)
        self.assertEqual(window, (40 * HOUR * 1000, 64 * HOUR * 1000))
        shown = tui.window_points(points, window)
        self.assertEqual([row["time"] / 1000 / HOUR for row in shown], [4, 80], "only the two neighbours")
        canvas, _ = tui.render_chart("cpu", shown, 400, 100, 1.0, window, 100)
        self.assertFalse(has_color(canvas, tui.rgb("#9ece6a")), "the gap must not be bridged")

    def test_a_jump_lands_on_real_samples_across_an_outage(self):
        points = [point(hour * HOUR, 50) for hour in range(5)]
        points += [point(hour * HOUR, 50, gap=hour == 80) for hour in range(80, 90)]
        view = view_of(points)
        view.key("home")
        for _ in range(3):
            view.key("ctrl-l")
        self.assertIn(view.cursor / 1000 / HOUR, [row["time"] / 1000 / HOUR for row in points])
        self.assertGreater(view.cursor, 4 * HOUR * 1000, "an outage must not trap the cursor")

    def test_switching_job_run_or_node_returns_to_the_newest_day(self):
        view = view_of(long_points())
        view.key("ctrl-h")
        view.clear_inspect()
        self.assertEqual((view.cursor, view.window_end), (None, None))
        second = job_data(long_points(), "456_2")
        view.update({"jobs": view.data["jobs"] + [{"job_id": "456_2", "connection": "Live", "runs": second["runs"],
                                                   "queue_status": None}], "job": view.data["job"],
                     "discovery_error": None})
        view.key("ctrl-h")
        view.key("tab")
        self.assertEqual((view.cursor, view.window_end), (None, None))


class TerminalSessionTests(unittest.TestCase):
    """Exercise the program through a terminal emulator stub."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        (self.base / "bin").mkdir()
        fake = self.base / "bin/ssh"
        fake.write_text("#!" + sys.executable + "\nimport json,os,sys,time\n"
                        "request = json.loads(sys.stdin.readline())\n"
                        "if request['operation'] == 'jobs':\n"
                        "    print(json.dumps({'schema_version': 1, 'jobs': []})); sys.exit(0)\n"
                        "with open(os.environ['SSH_PIDS'], 'a') as stream: stream.write(str(os.getpid()) + '\\n')\n"
                        "sys.stdout.write(open(os.environ['SSH_EVENTS']).read()); sys.stdout.flush()\n"
                        "time.sleep(60)\n")
        fake.chmod(0o700)
        (self.base / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events()))

    def session(self, answer="graphics", keys=(b"q",), args=(), size=(30, 100, 1000, 600), during=None, **extra):
        """Return (output, status, terminal flags)."""
        env = {key: value for key, value in os.environ.items() if key not in ("TMUX", "STY")
               and not key.startswith("HPC_JOBMON_")}
        env.update(PATH=str(self.base / "bin") + os.pathsep + os.environ["PATH"], TERM="xterm-256color",
                   SSH_EVENTS=str(self.base / "events.jsonl"), SSH_PIDS=str(self.base / "pids"))
        env.update(extra)
        command = [sys.executable, str(ROOT / "jobmon/live.py"), JOB_ID, "--host", "test-alias",
                   "--output-dir", str(self.base / "downloads")] + list(args)
        pid, fd = pty.fork()
        if pid == 0:  # pragma: no cover - replaced immediately
            try:
                fcntl.ioctl(0, termios.TIOCSWINSZ, struct.pack("HHHH", *size))
                os.chdir(str(self.base))
                os.execvpe(command[0], command, env)
            finally:
                os._exit(127)
        before = termios.tcgetattr(fd)
        output, answered, pending, status, deadline = b"", False, list(keys), None, time.time() + 20
        sent_during = during is None
        try:
            while time.time() < deadline:
                if not answered and tui.GRAPHICS_QUERY in output:
                    answered = True
                    os.write(fd, {"graphics": b"\x1b_Gi=31;OK\x1b\\\x1b[?62;22c", "device": b"\x1b[?62c",
                                  "silent": b""}[answer])
                ready = b"Run " + RUN_ID.encode() in output and b"Last interval" in output
                if ready and not sent_during:
                    sent_during = True
                    during(pid, fd)
                elif ready and pending and time.time() >= getattr(self, "next_key", 0):
                    os.write(fd, pending.pop(0))
                    self.next_key = time.time() + 0.1
                if select.select([fd], [], [], 0.02)[0]:
                    try:
                        output += os.read(fd, 65536)
                    except OSError:
                        pass
                finished, raw = os.waitpid(pid, os.WNOHANG)
                if finished:
                    status = raw
                    break
            else:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                self.fail("hpc-live did not finish; tail: {!r}".format(output[-400:]))
            while select.select([fd], [], [], 0.05)[0]:
                try:
                    chunk = os.read(fd, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                output += chunk
            after = termios.tcgetattr(fd)
        finally:
            os.close(fd)
        self.assertEqual(after[3], before[3], "terminal local modes were not restored")
        self.assertEqual(after[0], before[0], "terminal input modes were not restored")
        return output, os.WEXITSTATUS(status), after

    def assert_restored(self, output, pixels=True):
        tail = output[output.rindex(b"\x1b[?2026l"):]
        for code in (b"\x1b[?7h", b"\x1b[?25h", b"\x1b[?1049l"):
            self.assertIn(code, tail)
        self.assertEqual(tui.DELETE_IMAGES in tail, pixels)

    def assert_streams_closed(self):
        for pid in (self.base / "pids").read_text().split():
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid), 0)

    def test_default_view_is_pixels_in_a_terminal_that_supports_them(self):
        output, status, _ = self.session()
        self.assertEqual(status, 0, output[-400:])
        found = APC.findall(output)
        first = [control for control, _ in found if control.startswith(b"a=T")]
        self.assertEqual(len(first), 2)
        keys = dict(item.split(b"=") for item in first[0].split(b","))
        # 100x30 at 10x20 pixels: 91 columns and 11 chart rows.
        self.assertEqual((keys[b"s"], keys[b"v"], keys[b"c"], keys[b"r"]), (b"910", b"220", b"91", b"11"))
        self.assertNotIn("\u2840".encode(), output)
        self.assertLess(output.index(b"\x1b[?1049h"), output.index(tui.GRAPHICS_QUERY))
        self.assert_restored(output)
        self.assert_streams_closed()

    def test_a_terminal_without_graphics_gets_text_charts_and_a_notice(self):
        for answer in ("device", "silent"):
            with self.subTest(answer=answer):
                output, status, _ = self.session(answer)
                self.assertEqual(status, 0, output[-400:])
                self.assertNotIn(b"a=T", output)
                self.assertIn(b"Pixel charts need Kitty graphics", output)
                self.assertRegex(output.decode("utf-8", "replace"), "[\u2801-\u28ff]")
                self.assert_restored(output, pixels=False)
                self.assertEqual(output.count(b"\x1b_G"), 1)

    def test_text_flag_sends_no_graphics_commands(self):
        output, status, _ = self.session(args=("--text",))
        self.assertEqual(status, 0, output[-400:])
        self.assertNotIn(b"\x1b_G", output)
        self.assertNotIn(b"Pixel charts need", output)

    def test_multiplexers_are_never_sent_graphics_commands(self):
        for extra in ({"TMUX": "/tmp/synthetic,1,0"}, {"TERM": "screen-256color"}, {"TERM": "tmux-256color"}):
            with self.subTest(extra=extra):
                output, status, _ = self.session(**extra)
                self.assertEqual(status, 0, output[-400:])
                self.assertNotIn(b"\x1b_G", output)
                self.assertIn(b"tmux and screen do not pass pixel charts through", output)

    def test_ctrl_c_is_a_key_that_cancels_with_130(self):
        output, status, _ = self.session(keys=(b"\x03",))
        self.assertEqual(status, 130, output[-400:])
        self.assert_restored(output)
        self.assert_streams_closed()

    def test_a_split_home_sequence_reaches_the_first_sample(self):
        output, status, _ = self.session(keys=(b"\x1b[", b"H", b"q"))
        self.assertEqual(status, 0, output[-400:])
        self.assertIn("at 0:00:00", SGR.sub("", output.decode("utf-8", "replace")))
        self.assert_restored(output)

    def test_ctrl_z_cannot_suspend_the_view_mid_screen(self):
        modes = []
        output, status, _ = self.session(keys=(b"\x1a", b"q"),
                                         during=lambda _pid, fd: modes.append(termios.tcgetattr(fd)[3]))
        self.assertEqual(status, 0, output[-400:])
        self.assertFalse(modes[0] & (termios.ISIG | termios.ECHO | termios.ICANON))

    def test_sigterm_still_restores_the_terminal(self):
        output, status, _ = self.session(keys=(), during=lambda pid, _fd: os.kill(pid, signal.SIGTERM))
        self.assertEqual(status, 128 + signal.SIGTERM, output[-400:])
        self.assert_restored(output)
        self.assert_streams_closed()

    def test_ctrl_h_and_ctrl_l_scroll_a_long_history_in_a_real_session(self):
        rows = [sample(hour * 3600) for hour in range(49)]
        for row in rows:
            row["interval_seconds"] = 3600
        (self.base / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events(rows)))
        output, status, _ = self.session(keys=(b"\x08",) * 6 + (b"\x0c", b"q"))
        self.assertEqual(status, 0, output[-400:])
        text = SGR.sub("", output.decode("utf-8", "replace"))
        self.assertIn("Showing the newest 24 hours of 48:00:00", text)
        self.assertIn("Showing 12:00:00 to 36:00:00 of 48:00:00", text)
        self.assertIn("at 12:00:00", text)
        self.assertIn("at 18:00:00", text)

    def test_a_resize_clears_and_redraws_at_the_new_size(self):
        def resize(_pid, fd):
            fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 800, 480))

        output, status, _ = self.session(keys=(b"h", b"q"), during=resize)
        self.assertEqual(status, 0, output[-400:])
        placed = [dict(item.split(b"=") for item in control.split(b",")) for control, _ in APC.findall(output)
                  if control.startswith(b"a=T")]
        self.assertGreater(len(placed), 2)
        self.assertEqual(placed[-1][b"c"], b"71", "images were not redrawn for the new width")
        self.assertGreaterEqual(output.count(b"\x1b[2J"), 3)

    def test_a_crash_in_the_view_leaves_a_usable_terminal_and_a_readable_error(self):
        script = self.base / "crash.py"
        script.write_text(
            "import sys\nsys.path.insert(0, {!r})\nimport tui\n"
            "def broken(self, *args):\n    raise RuntimeError('deliberate view failure')\n"
            "tui.Dashboard.compose = broken\nimport live\n"
            "sys.exit(live.main(sys.argv[1:]))\n".format(str(ROOT / "jobmon")))
        env = dict(os.environ, PATH=str(self.base / "bin") + os.pathsep + os.environ["PATH"],
                   SSH_EVENTS=str(self.base / "events.jsonl"), SSH_PIDS=str(self.base / "pids"))
        pid, fd = pty.fork()
        if pid == 0:  # pragma: no cover - replaced immediately
            try:
                os.execvpe(sys.executable, [sys.executable, str(script), JOB_ID, "--host", "test-alias", "--text"], env)
            finally:
                os._exit(127)
        before = termios.tcgetattr(fd)
        output, status, deadline = b"", None, time.time() + 15
        while time.time() < deadline and status is None:
            if select.select([fd], [], [], 0.02)[0]:
                try:
                    output += os.read(fd, 65536)
                except OSError:
                    pass
            finished, raw = os.waitpid(pid, os.WNOHANG)
            if finished:
                status = raw
        self.assertIsNotNone(status, "the failed view hung")
        after = termios.tcgetattr(fd)
        os.close(fd)
        self.assertEqual(after[3], before[3])
        tail = output[output.rindex(b"\x1b[?7h"):]
        self.assertIn(b"\x1b[?1049l", tail)
        self.assertIn(b"deliberate view failure", tail, "the traceback must appear after the screen is restored")

    def test_without_a_terminal_it_points_at_web_and_starts_nothing(self):
        env = dict(os.environ, PATH=str(self.base / "bin") + os.pathsep + os.environ["PATH"],
                   SSH_EVENTS=str(self.base / "events.jsonl"), SSH_PIDS=str(self.base / "pids"))
        result = subprocess.run([sys.executable, str(ROOT / "jobmon/live.py"), JOB_ID, "--host", "test-alias"],
                                env=env, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=15)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--web", result.stderr)
        self.assertFalse((self.base / "pids").exists())

    def test_web_only_options_need_web(self):
        for extra, message in ((["--port", "8080"], "only with --web"), (["--no-browser"], "only with --web"),
                               (["--web", "--text"], "terminal view, not --web")):
            with self.subTest(extra=extra), patch.object(sys, "stderr") as stderr:
                self.assertEqual(live.main([JOB_ID, "--host", "test-alias"] + extra), 2)
                self.assertIn(message, "".join(call.args[0] for call in stderr.write.call_args_list))


if __name__ == "__main__":
    unittest.main()
