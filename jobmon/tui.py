"""Terminal live charts with Kitty pixels or braille."""

import base64
import bisect
from collections import namedtuple
import contextlib
import fcntl
import functools
import io
import math
import os
import re
import select
import signal
import struct
import sys
import termios
import textwrap
import time
import unicodedata
import zlib

from jobmon import display_job_name, printable_text, timestamp_seconds
from plot import CHART_WINDOW_MS, EXPORT_SPEC, completion_result, write_exports
from slurm_backend import JobmonError

SERIES = EXPORT_SPEC["series"]
TITLES = {"cpu": "CPU", "memory": "Memory"}
UNITS = {"cpu": "%", "memory": " GiB"}
MIN_COLS, MIN_ROWS = 50, 16
GUTTER = 8
REFRESH_SECONDS = 1
HEARTBEAT_SECONDS = 5
IDLE_SECONDS = 0.25
MESSAGE_SECONDS = 10
DETECT_SECONDS = 1
ESCAPE_SECONDS = 0.05
MAX_KEY_SEQUENCE_BYTES = 32
CHUNK_BYTES = 4096
MAX_IMAGE = (1600, 700)
DEFAULT_CELL = (10, 20)
IMAGE_IDS = {"cpu": 7301, "memory": 7302}
GRAPHICS_QUERY = b"\x1b_Gi=31,s=1,v=1,a=q,t=d,f=24;AAAA\x1b\\\x1b[c"
GRAPHICS_REPLY = b"\x1b_Gi=31;OK"
DELETE_IMAGES = b"\x1b_Ga=d,d=A,q=2\x1b\\"
DEVICE_REPLY = re.compile(rb"\x1b\[\?[0-9;]*c")
KEY_SEQUENCES = {b"\x1b[A": "up", b"\x1b[B": "down", b"\x1bOA": "up", b"\x1bOB": "down",
                 b"\x1b[H": "home", b"\x1b[F": "end", b"\x1b[1~": "home", b"\x1b[4~": "end",
                 b"\x1bOH": "home", b"\x1bOF": "end", b"\x1b[Z": "backtab"}
TEXT_KEYS = {b"\t": "tab", b"\x03": "ctrl-c", b"\x08": "ctrl-h", b"\x0c": "ctrl-l"}
# (direction, window fraction); zero moves one sample.
INSPECT_MOVES = {"h": (-1, 0), "l": (1, 0), "H": (-1, 1 / 20), "L": (1, 1 / 20),
                 "ctrl-h": (-1, 1 / 4), "ctrl-l": (1, 1 / 4)}
GREEN, RED, AMBER, DIM = "38;5;150", "38;5;210", "38;5;215", "2"
KIND_STYLES = {"finished": GREEN, "failed": RED, "unknown": AMBER}
TEXT_COLORS = {"#7aa2f7": "38;5;111", "#ff9e64": AMBER, "#9ece6a": GREEN}
RUNTIME_LABELS = {"slurm": "Elapsed", "monitoring": "Monitored", "samples": "Sample span"}
BRAILLE_BITS = ((0x01, 0x02, 0x04, 0x40), (0x08, 0x10, 0x20, 0x80))
NOTICE = ("Live saved samples. CPU averages cover sampled intervals only. Missing readings remain gaps. "
          "Memory peaks stay per node. Quitting stops viewing, while workload collection continues.")


def rgb(value):
    return tuple(int(value[index:index + 2], 16) for index in (1, 3, 5))


BACKGROUND, GRID, REFERENCE = rgb("#1d202f"), rgb("#343a55"), rgb("#737aa2")
Frame = namedtuple("Frame", "lines images")
Placement = namedtuple("Placement", "chart points top left cols rows cell cursor signature window maximum")


def cell_width(char):
    if unicodedata.combining(char):
        return 0
    return 2 if unicodedata.east_asian_width(char) in ("W", "F") else 1


def fit(spans, width):
    """Sanitize saved text and fit styled spans to terminal cells."""
    output, room = [], width
    for text, style in spans:
        kept = []
        for char in printable_text(str(text)):
            size = cell_width(char)
            if size > room:
                room = 0
                break
            kept.append(char)
            room -= size
        if kept:
            output.append("\x1b[{}m{}\x1b[0m".format(style, "".join(kept)) if style else "".join(kept))
        if not room:
            break
    return "".join(output)


def chart_scale(chart, points):
    keys = [spec[0] for spec in SERIES[chart]]
    peak = max((point[key] for point in points for key in keys if point[key] is not None), default=0)
    if chart == "cpu":
        peak = max(peak, 100)
        return 100 if peak == 100 else peak * 1.1
    return peak * 1.1 if peak > 0 else 1


def chart_window(points, window_end=None):
    """Follow newest samples unless window_end is set."""
    first, last = points[0]["time"], points[-1]["time"]
    if last - first <= CHART_WINDOW_MS:
        return first, last
    end = last if window_end is None else max(first + CHART_WINDOW_MS, min(last, window_end))
    return end - CHART_WINDOW_MS, end


def window_points(points, window):
    times = [point["time"] for point in points]
    return points[max(0, bisect.bisect_left(times, window[0]) - 1):bisect.bisect_right(times, window[1]) + 1]


def nearest_index(times, when):
    index = bisect.bisect_left(times, when)
    if index == len(times) or (index and when - times[index - 1] < times[index] - when):
        index -= 1
    return index


def has_readings(chart, points):
    return any(point[spec[0]] is not None for point in points for spec in SERIES[chart])


def polylines(points, key, x_of, y_of):
    """Keep per-column extremes so single-sample spikes survive."""
    runs, current, bucket, column = [], [], [], None

    def close_column():
        if bucket:
            low = min(range(len(bucket)), key=lambda index: bucket[index][1])
            high = max(range(len(bucket)), key=lambda index: bucket[index][1])
            for index in (0, min(low, high), max(low, high), len(bucket) - 1):
                if not current or current[-1] != bucket[index]:
                    current.append(bucket[index])
            del bucket[:]

    def close_run():
        nonlocal current, column
        close_column()
        if current:
            runs.append(current)
        current, column = [], None

    for point in points:
        value = point[key]
        if point.get("gap") or value is None:
            close_run()
        if value is None:
            continue
        x = x_of(point["time"])
        if int(x) != column:
            close_column()
            column = int(x)
        bucket.append((x, y_of(value)))
    close_run()
    return runs


@functools.lru_cache(maxsize=512)
def blend_table(value, level):
    return bytes(int(old + (value - old) * level / 32) for old in range(256))


class Canvas:

    def __init__(self, width, height, color):
        self.width, self.height = width, height
        self.pixels = bytearray(bytes(color) * (width * height))

    def copy(self):
        duplicate = Canvas.__new__(Canvas)
        duplicate.width, duplicate.height, duplicate.pixels = self.width, self.height, bytearray(self.pixels)
        return duplicate

    def horizontal(self, y, color, dash=0):
        row = y * self.width * 3
        solid = bytes(color)
        if not dash:
            self.pixels[row:row + self.width * 3] = solid * self.width
            return
        for x in range(0, self.width, 2 * dash):
            end = min(x + dash, self.width)
            self.pixels[row + x * 3:row + end * 3] = solid * (end - x)

    def vertical(self, x, color, dash, width=1):
        solid = bytes(color)
        for y in range(self.height):
            if y % (2 * dash) < dash:
                for offset in range(max(0, x), min(self.width, x + width)):
                    index = (y * self.width + offset) * 3
                    self.pixels[index:index + 3] = solid

    def blend(self, x, y, color, strength):
        if 0 <= y < self.height and strength > 0:
            index = (y * self.width + x) * 3
            for channel in range(3):
                old = self.pixels[index + channel]
                self.pixels[index + channel] = int(old + (color[channel] - old) * min(1, strength))

    def span(self, x, top, bottom, color, strength):
        if not 0 <= x < self.width:
            return
        top, bottom = max(-0.5, top), min(self.height - 0.5, bottom)
        first, last = math.ceil(top + 0.5), math.floor(bottom - 0.5)
        if first > last:
            for y in range(int(round(top)), int(round(bottom)) + 1):
                self.blend(x, y, color, (min(bottom, y + 0.5) - max(top, y - 0.5)) * strength)
            return
        count, stride = last - first + 1, self.width * 3
        begin = (first * self.width + x) * 3
        for channel, value in enumerate(color):
            slots = slice(begin + channel, begin + channel + (count - 1) * stride + 1, stride)
            if strength >= 1:
                self.pixels[slots] = bytes((value,)) * count
            else:
                self.pixels[slots] = self.pixels[slots].translate(blend_table(value, int(strength * 32)))
        self.blend(x, first - 1, color, (first - 0.5 - top) * strength)
        self.blend(x, last + 1, color, (bottom - last - 0.5) * strength)

    def line(self, run, color, radius):
        """Rasterize per column, independent of sample count."""
        spans = {}

        def cover(column, low, high):
            low, high = min(low, high), max(low, high)
            known = spans.get(column)
            spans[column] = (min(low, known[0]), max(high, known[1])) if known else (low, high)

        if len(run) == 1:
            cover(int(round(run[0][0])), run[0][1], run[0][1])
        for (ax, ay), (bx, by) in zip(run, run[1:]):
            left, right = min(ax, bx), max(ax, bx)
            slope = (by - ay) / (bx - ax) if bx != ax else None
            for column in range(max(0, math.floor(left + 0.5)), min(self.width, math.floor(right + 0.5) + 1)):
                if slope is None:
                    cover(column, ay, by)
                else:
                    start, stop = max(left, column - 0.5), min(right, column + 0.5)
                    cover(column, ay + slope * (start - ax), ay + slope * (stop - ax))
        # Paint soft edges before neighbouring solid cores.
        for distance in range(math.ceil(radius + 0.5), -1, -1):
            strength = min(1, radius + 0.5 - distance)
            cap = math.sqrt(max(0, radius * radius - distance * distance))
            if strength > 0:
                for column, (low, high) in spans.items():
                    self.span(column - distance, low - cap, high + cap, color, strength)
                    if distance:
                        self.span(column + distance, low - cap, high + cap, color, strength)


def render_chart(chart, points, width, height, radius, window=None, maximum=None):
    canvas = Canvas(width, height, BACKGROUND)
    pad = math.ceil(radius) + 2
    maximum = maximum or chart_scale(chart, points)
    first, last = window or (points[0]["time"], points[-1]["time"])
    span = max(1000, last - first)

    def x_of(when):
        return pad + (when - first) / span * (width - 1 - 2 * pad)

    def y_of(value):
        return height - 1 - pad - value / maximum * (height - 1 - 2 * pad)

    for tick in range(5):
        canvas.horizontal(int(y_of(maximum * tick / 4)), GRID)
    if chart == "cpu":
        canvas.horizontal(int(y_of(100)), REFERENCE, max(3, int(radius * 3)))
    for key, _, color in SERIES[chart]:
        for run in polylines(points, key, x_of, y_of):
            canvas.line(run, rgb(color), radius)
    return canvas, x_of


def image_commands(image_id, canvas, cols, rows):
    payload = base64.standard_b64encode(zlib.compress(bytes(canvas.pixels), 1))
    chunks = [payload[start:start + CHUNK_BYTES] for start in range(0, len(payload), CHUNK_BYTES)]
    head = "a=T,f=24,o=z,s={},v={},i={},p=1,c={},r={},C=1,q=2".format(canvas.width, canvas.height, image_id, cols, rows)
    commands = []
    for index, chunk in enumerate(chunks):
        more = int(index + 1 < len(chunks))
        control = "{},m={}".format(head, more) if index == 0 else "m={},q=2".format(more)
        commands.append(b"\x1b_G" + control.encode("ascii") + b";" + chunk + b"\x1b\\")
    return b"".join(commands)


def text_chart(chart, points, cols, rows, cursor=None, window=None, maximum=None):
    """Braille chart: 2x4 dots per cell."""
    maximum = maximum or chart_scale(chart, points)
    wide, tall = cols * 2, rows * 4
    first, last = window or (points[0]["time"], points[-1]["time"])
    span = max(1000, last - first)

    def x_of(when):
        return (when - first) / span * (wide - 1)

    def y_of(value):
        return tall - 1 - value / maximum * (tall - 1)

    bits = [[0] * cols for _ in range(rows)]
    owners = [[None] * cols for _ in range(rows)]

    def dot(x, y, style):
        if not 0 <= x < wide:
            return
        cell, row = x // 2, y // 4
        bits[row][cell] |= BRAILLE_BITS[x % 2][y % 4]
        if owners[row][cell] is None:
            owners[row][cell] = style

    for key, _, color in SERIES[chart]:
        style = TEXT_COLORS[color]
        for run in polylines(points, key, x_of, y_of):
            previous = None
            for x, y in run:
                x, y = int(round(x)), int(round(y))
                steps = max(abs(x - previous[0]), abs(y - previous[1])) if previous else 0
                for step in range(steps + 1):
                    dot(x if not steps else previous[0] + (x - previous[0]) * step // steps,
                        y if not steps else previous[1] + (y - previous[1]) * step // steps, style)
                previous = (x, y)
    if chart == "cpu":
        y = int(round(y_of(100)))
        for x in range(0, wide, 4):
            if not bits[y // 4][x // 2]:
                dot(x, y, DIM)
    cursor_cell = int(x_of(cursor)) // 2 if cursor is not None else None
    lines = []
    for row in range(rows):
        cells, style, run = [], None, []
        for column in range(cols):
            current = owners[row][column]
            if column == cursor_cell:
                current = "48;5;238" if current is None else current + ";48;5;238"
            if current != style:
                cells.append(("".join(run), style))
                style, run = current, []
            run.append(chr(0x2800 + bits[row][column]) if bits[row][column] else " ")
        cells.append(("".join(run), style))
        lines.append("".join("\x1b[{}m{}\x1b[0m".format(item, text) if item else text for text, item in cells if text))
    return lines


def tick_label(chart, value):
    return "{:g}{}".format(float("{:.3g}".format(value)), "%" if chart == "cpu" else "")


def gutter_labels(chart, maximum, rows):
    labels = {}
    for tick in range(5):
        value = maximum * tick / 4
        labels[min(rows - 1, int((1 - value / maximum) * rows))] = tick_label(chart, value)
    return [fit([(labels.get(row, "").rjust(GUTTER - 1) + " ", DIM)], GUTTER) for row in range(rows)]


def elapsed_label(seconds):
    seconds = max(0, int(round(seconds)))
    return "{}:{:02d}:{:02d}".format(seconds // 3600, seconds // 60 % 60, seconds % 60)


def time_label(when, first, axis, long_span=False):
    if axis == "elapsed":
        return elapsed_label((when - first) / 1000)
    return time.strftime("%m-%d %H:%M" if long_span else "%H:%M:%S", time.localtime(when / 1000))


def time_axis(points, axis, cols, window=None):
    first = points[0]["time"]
    start, end = window or (first, points[-1]["time"])
    long_span = end - start >= CHART_WINDOW_MS
    labels = [time_label(start + (end - start) * tick / 4, first, axis, long_span) for tick in range(5)]
    size = max(len(label) for label in labels)
    ticks = range(5) if (size + 2) * 5 <= cols else (0, 2, 4) if (size + 2) * 3 <= cols else (0, 4)
    row = [" "] * cols
    for tick in ticks:
        label = labels[tick]
        start = (0 if tick == 0 else cols - len(label) if tick == 4 else cols * tick // 4 - len(label) // 2)
        row[start:start + len(label)] = label
    return "".join(row).rstrip()


def wrap(text, width):
    return textwrap.wrap(printable_text(str(text)), max(1, width), break_long_words=True) or [""]


class Dashboard:

    def __init__(self, pixels, notice=None, output_dir="downloads"):
        self.pixels, self.output_dir = pixels, output_dir
        self.data = None
        self.selected = None
        self.views = {}
        self.axis = "elapsed"
        self.cursor = None
        self.window_end = None
        self.details = False
        self.scroll = 0
        self.stale = True
        self.notes = [notice] if notice else []
        self.message = None
        self.exit_status = None
        if notice:
            self.notify(notice, AMBER)

    def notify(self, text, style=""):
        self.message = (text, style, time.monotonic() + MESSAGE_SECONDS)

    def update(self, data):
        self.data = data
        if self.selected is None:
            self.selected = data["jobs"][0]["job_id"]
        self.stale = False

    def job_ids(self):
        return [job["job_id"] for job in self.data["jobs"]] if self.data else []

    def selection(self):
        job = self.data["job"] if self.data else None
        if job is None or not job["runs"]:
            return job, None, None
        view = self.views.get(job["job_id"], {})
        run = next((item for item in job["runs"] if item["run_id"] == view.get("run")), job["runs"][-1])
        node = next((item for item in run["nodes"] if item["node"] == view.get("node")),
                    run["nodes"][0] if run["nodes"] else None)
        return job, run, node

    def step_job(self, offset):
        ids = self.job_ids()
        if len(ids) > 1:
            self.selected = ids[(ids.index(self.selected) + offset) % len(ids)]
            self.clear_inspect()
            self.stale = True

    def step_run(self, offset):
        job, run, _ = self.selection()
        if run is not None and len(job["runs"]) > 1:
            index = (job["runs"].index(run) + offset) % len(job["runs"])
            chosen = None if index == len(job["runs"]) - 1 else job["runs"][index]["run_id"]
            self.views.setdefault(job["job_id"], {}).update(run=chosen, node=None)
            self.clear_inspect()

    def step_node(self, offset):
        job, run, node = self.selection()
        if node is not None and len(run["nodes"]) > 1:
            next_node = run["nodes"][(run["nodes"].index(node) + offset) % len(run["nodes"])]
            self.views.setdefault(job["job_id"], {})["node"] = next_node["node"]
            self.clear_inspect()

    def clear_inspect(self):
        self.cursor, self.window_end = None, None

    def pin_window(self, points):
        start, end = chart_window(points, self.window_end)
        self.window_end = (self.cursor + CHART_WINDOW_MS if self.cursor < start
                           else self.cursor if self.cursor > end else end)

    def move_cursor(self, key):
        node = self.selection()[2]
        if node is None or not node["points"]:
            return
        points = node["points"]
        times = [point["time"] for point in points]
        if key in ("home", "end"):
            target = 0 if key == "home" else len(times) - 1
        else:
            direction, fraction = INSPECT_MOVES[key]
            if self.cursor is None:
                self.cursor = times[-1]
                if not fraction:
                    self.pin_window(points)
                    return
            start, end = chart_window(points, self.window_end)
            index = nearest_index(times, self.cursor)
            target = index + direction
            if fraction:
                landed = nearest_index(times, self.cursor + direction * (end - start) * fraction)
                target = min(landed, target) if direction < 0 else max(landed, target)
        self.cursor = times[min(len(times) - 1, max(0, target))]
        self.pin_window(points)

    def save(self):
        job, run, node = self.selection()
        if node is None or not node["points"]:
            self.notify("No saved samples to export.", AMBER)
            return
        try:
            paths = write_exports(job, self.output_dir, selections=[(run, node)], axis=self.axis)
        except (OSError, JobmonError, ValueError) as exc:
            self.notify("Save failed: " + str(exc), RED)
        else:
            self.notify("Saved {} ZIPs (image + data) to {}".format(len(paths), self.output_dir), GREEN)

    def key(self, key):
        if key in ("q", "ctrl-c"):
            self.exit_status = 130 if key == "ctrl-c" else 0
            return True
        actions = {"tab": lambda: self.step_job(1), "backtab": lambda: self.step_job(-1),
                   "r": lambda: self.step_run(1), "R": lambda: self.step_run(-1),
                   "n": lambda: self.step_node(1), "N": lambda: self.step_node(-1),
                   "s": self.save}
        if key in actions:
            actions[key]()
        elif key == "a":
            self.axis = "clock" if self.axis == "elapsed" else "elapsed"
        elif key == "d":
            self.details, self.scroll = not self.details, 0
        elif self.details and key in ("up", "down"):
            self.scroll = max(0, self.scroll + (1 if key == "down" else -1))
        elif key == "esc":
            self.details = False
            self.clear_inspect()
        elif (key in INSPECT_MOVES or key in ("home", "end")) and not self.details:
            self.move_cursor(key)
        return False

    def tabs(self, cols):
        items = []
        for job in self.data["jobs"]:
            run = job["runs"][-1] if job["runs"] else None
            result = completion_result(job["job_id"], run, job["queue_status"])
            text = " {}{} ".format(job["job_id"], " · " + result["label"] if result else "")
            style = (KIND_STYLES[result["kind"]] if result else "" if job["connection"] == "Live"
                     else DIM if job["connection"] == "Connecting" else AMBER)
            chosen = job["job_id"] == self.selected
            items.append((text, ";".join(part for part in ("7" if chosen else "", style) if part)))
        chosen = self.job_ids().index(self.selected)
        start = 0
        while sum(len(item[0]) + 1 for item in items[start:chosen + 1]) > cols - 4:
            start += 1
        spans = [("< ", DIM)] if start else []
        for item in items[start:]:
            spans.extend((item, (" ", "")))
        return fit(spans, cols)

    def title(self, job, run, cols):
        queue = (job.get("scheduler") or {}).get("queue") or [{}]
        name = (run or {}).get("job_name") or queue[0].get("job_name")
        runtime = (run or {}).get("runtime") or {}
        elapsed = ("{} {}".format(RUNTIME_LABELS[runtime["source"]], runtime["display"])
                   if runtime.get("seconds") is not None else "Elapsed unavailable")
        spans = [("Job {}".format(job["job_id"]), "1")]
        if name:
            spans.append((" ({})".format(display_job_name(name)), ""))
        connection = job.get("connection") or "Connecting"
        spans += [("   " + elapsed + "   ", ""), (connection, GREEN if connection == "Live" else AMBER)]
        status, snapshot = job.get("queue_status"), job.get("scheduler_snapshot_at")
        if status:
            spans.append(("   Slurm queue: {} at {}".format(
                status["state"] or "job not present", self.stamp(status["checked_at"])), DIM))
        elif queue[0].get("state"):
            spans.append(("   Initial Slurm snapshot: {}{}".format(
                queue[0]["state"], " at " + self.stamp(snapshot) if snapshot else ""), DIM))
        return fit(spans, cols)

    @staticmethod
    def stamp(value):
        seconds = timestamp_seconds(value)
        return time.strftime("%H:%M:%S", time.localtime(seconds)) if seconds is not None else "unknown time"

    def context(self, job, run, node, cols):
        if run is None:
            return fit([("Connecting to saved history...", DIM)], cols)
        runs, nodes = job["runs"], run["nodes"]
        # Keep missing nodes visible in narrow terminals.
        missing = [name for name in run["expected_nodes"] if not any(item["node"] == name for item in nodes)]
        lead = [("Missing: " + ", ".join(missing) + "  ", AMBER)] if missing else []
        spans = ["Run " + run["run_id"] + (" ({}/{})".format(runs.index(run) + 1, len(runs)) if len(runs) > 1 else "")]
        if node is not None:
            position = " ({}/{})".format(nodes.index(node) + 1, len(nodes)) if len(nodes) > 1 else ""
            spans.append(node["node"] + position)
            spans.append("/".join(node["scopes"]))
        spans.append("{}/{} nodes".format(
            len(nodes), run.get("expected_node_count") or len(run["expected_nodes"]) or "?"))
        spans.append("No wrapper completion record" if run["exit_code"] is None
                     else "Wrapper exit: {}".format(run["exit_code"]))
        return fit(lead + [(" · ".join(spans), DIM)], cols)

    def legend(self, chart, points):
        spans = [(TITLES[chart], "1")]
        if not points:
            return spans
        point = points[-1]
        if self.cursor is not None:
            point = points[nearest_index([item["time"] for item in points], self.cursor)]
            long_span = points[-1]["time"] - points[0]["time"] >= CHART_WINDOW_MS
            label = time_label(point["time"], points[0]["time"], self.axis, long_span)
            spans.append(("  at {}{}".format(label, " (final sample)" if point["final"] else ""), ""))
        for key, label, color in SERIES[chart]:
            value = point[key]
            shown = "unavailable" if value is None else "{:.2f}{}".format(value, UNITS[chart])
            spans.append(("  {} {}".format(label, shown), TEXT_COLORS[color]))
        return spans

    def chart_block(self, chart, node, job, run, top, cols, height, cell):
        width = cols - GUTTER - 1
        points = node["points"] if node else []
        lines = [fit(self.legend(chart, points), cols)]
        if not points or not has_readings(chart, points):
            text = "No available measurements" if points else "Waiting for saved samples"
            lines += ["" if row != height // 2 else fit([(" " * GUTTER + text, DIM)], cols) for row in range(height)]
            return lines + [""], None
        # Keep scales fixed while scrolling.
        window, maximum = chart_window(points, self.window_end), chart_scale(chart, points)
        shown = window_points(points, window)
        labels = gutter_labels(chart, maximum, height)
        placement = None
        if self.pixels:
            signature = (job["job_id"], run["run_id"], node["node"], len(points), points[-1]["time"], window, maximum)
            placement = Placement(chart, shown, top + 1, GUTTER, width, height, cell, self.cursor, signature,
                                  window, maximum)
            lines += labels
        else:
            rows = text_chart(chart, shown, width, height, self.cursor, window, maximum)
            lines += [label + row for label, row in zip(labels, rows)]
        lines.append(fit([(" " * GUTTER + time_axis(points, self.axis, width, window), DIM)], cols))
        return lines, placement

    def window_line(self, node, cols):
        points = node["points"] if node else []
        if not points or points[-1]["time"] - points[0]["time"] <= CHART_WINDOW_MS:
            return None
        first, last = points[0]["time"], points[-1]["time"]
        total = elapsed_label((last - first) / 1000)
        if self.cursor is None:
            text = "Showing the newest 24 hours of {}. h starts inspecting; Ctrl-h jumps back.".format(total)
        else:
            start, end = chart_window(points, self.window_end)
            text = "Showing {} to {} of {}. Esc returns to the newest.".format(
                time_label(start, first, self.axis, True), time_label(end, first, self.axis, True), total)
        return fit([(text, DIM)], cols)

    def detail_lines(self, job, node, cols, height):
        lines = []
        scopes = ["Scope {}: {}".format(scope, job["scope_notes"].get(scope, "CPU/memory unavailable; see diagnostics"))
                  for scope in (node["scopes"] if node else [])]
        for text in [NOTICE] + self.notes + scopes:
            lines.extend(wrap(text, cols))
        problems = list(job.get("diagnostics") or []) if job else []
        if self.data.get("discovery_error"):
            problems.append(self.data["discovery_error"])
        lines.append("")
        lines.append("Diagnostics ({})".format(len(problems)) if problems else "No recorded diagnostics.")
        for problem in problems:
            lines.extend(wrap("- " + problem, cols))
        self.scroll = min(self.scroll, max(0, len(lines) - height))
        return [fit([(line, "")], cols) for line in lines[self.scroll:self.scroll + height]]

    def footer(self, job, run, node, cols):
        if self.message and time.monotonic() < self.message[2]:
            return fit([(self.message[0], self.message[1])], cols)
        keys = []
        if len(self.job_ids()) > 1:
            keys.append("Tab job")
        if run is not None and len(job["runs"]) > 1:
            keys.append("r run")
        if run is not None and len(run["nodes"]) > 1:
            keys.append("n node")
        keys += ["a axis", "h/l inspect", "Ctrl-h/l jump", "s save"]
        count = len(job.get("diagnostics") or []) if job else 0
        spans = [("  ".join(keys) + "  ", DIM), ("d details" + (" ({})".format(count) if count else ""),
                                                 AMBER if count else DIM), ("  q quit", DIM)]
        return fit(spans, cols)

    def compose(self, cols, rows, cell):
        if cols < MIN_COLS or rows < MIN_ROWS:
            text = "Terminal too small for hpc-live: need {}x{}, have {}x{}.".format(MIN_COLS, MIN_ROWS, cols, rows)
            return Frame([fit([(text, AMBER)], cols)] + [""] * (rows - 1), [])
        if self.data is None:
            return Frame([fit([("Connecting to saved history...", DIM)], cols)] + [""] * (rows - 1), [])
        job, run, node = self.selection()
        top = [self.tabs(cols)] if len(self.data["jobs"]) > 1 else []
        top.append(self.title(job, run, cols))
        top.append(self.context(job, run, node, cols))
        latest = bool(job["runs"]) and run is job["runs"][-1]
        result = completion_result(job["job_id"], run, job.get("queue_status"), latest or not job["runs"])
        if result:
            top.append(fit([(result["message"] + " " + result["detail"], KIND_STYLES[result["kind"]])], cols))
        window_line = None if self.details else self.window_line(node, cols)
        if window_line:
            top.append(window_line)
        body_rows = rows - len(top) - 1
        images = []
        if self.details:
            body = self.detail_lines(job, node, cols, body_rows)
        else:
            height = (body_rows - 4) // 2
            body = []
            for chart in ("cpu", "memory"):
                lines, placement = self.chart_block(chart, node, job, run, len(top) + len(body), cols, height, cell)
                body += lines
                if placement:
                    images.append(placement)
        lines = top + body + [""] * (body_rows - len(body)) + [self.footer(job, run, node, cols)]
        return Frame(lines[:rows], images)


class KeyDecoder:
    """Buffer split keys; discard replies without retaining payloads."""

    def __init__(self):
        self.pending = b""
        self.deadline = None
        self.in_reply = False
        self.reply_escape = False

    def wait_seconds(self):
        if self.deadline is None:
            return IDLE_SECONDS
        return min(IDLE_SECONDS, max(0, self.deadline - time.monotonic()))

    def feed(self, data=b"", now=None):
        now = time.monotonic() if now is None else now
        keys = []
        if self.deadline is not None and now >= self.deadline:
            keys.append("esc")
            self.pending, self.deadline = b"", None
        for value in data:
            byte = bytes((value,))
            if self.in_reply:
                if self.reply_escape and byte == b"\\":
                    self.in_reply = False
                self.reply_escape = byte == b"\x1b"
                continue
            if self.pending == b"\x1b":
                self.deadline = None
                if byte == b"_":
                    self.pending, self.in_reply = b"", True
                    continue
                if byte in (b"[", b"O"):
                    self.pending += byte
                    continue
                keys.append("esc")
                self.pending = b""
            elif self.pending:
                complete = (0x20 <= value <= 0x7e if self.pending == b"\x1bO" else 0x40 <= value <= 0x7e)
                if complete:
                    key = KEY_SEQUENCES.get(self.pending + byte)
                    if key is not None:
                        keys.append(key)
                    self.pending = b""
                    continue
                if 0x20 <= value <= 0x3f:
                    if len(self.pending) < MAX_KEY_SEQUENCE_BYTES:
                        self.pending += byte
                    continue
                self.pending = b""
            if byte == b"\x1b":
                self.pending, self.deadline = byte, now + ESCAPE_SECONDS
            elif byte in TEXT_KEYS:
                keys.append(TEXT_KEYS[byte])
            elif b" " <= byte <= b"~":
                keys.append(byte.decode("ascii"))
        return keys


class Terminal:

    def __init__(self, input_fd=0, output_fd=1):
        self.input, self.output = input_fd, output_fd
        self.decoder = KeyDecoder()
        self.saved = None
        self.graphics = False
        self.lines = []
        self.shown = {}
        self.bases = {}

    def write(self, data):
        view = memoryview(data)
        while view:
            view = view[os.write(self.output, view):]

    def enter(self):
        self.saved = termios.tcgetattr(self.input)
        attributes = termios.tcgetattr(self.input)
        attributes[0] &= ~termios.IXON
        # Read Ctrl-C/Ctrl-Z as keys; prevent suspension.
        attributes[3] &= ~(termios.ECHO | termios.ICANON | termios.ISIG | termios.IEXTEN)
        attributes[6][termios.VMIN], attributes[6][termios.VTIME] = 1, 0
        termios.tcsetattr(self.input, termios.TCSAFLUSH, attributes)
        self.write(b"\x1b[?1049h\x1b[?25l\x1b[?7l\x1b[2J")

    def leave(self):
        if self.saved is None:
            return
        try:
            self.write((DELETE_IMAGES if self.graphics else b"") + b"\x1b[?7h\x1b[?25h\x1b[?1049l")
        finally:
            termios.tcsetattr(self.input, termios.TCSADRAIN, self.saved)
            termios.tcflush(self.input, termios.TCIFLUSH)
            self.saved = None

    def detect_graphics(self):
        """Return (supported, typed keys), fenced by device attributes."""
        self.write(GRAPHICS_QUERY)
        reply, deadline = b"", time.monotonic() + DETECT_SECONDS
        while not DEVICE_REPLY.search(reply):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self.input], [], [], remaining)[0]:
                break
            chunk = os.read(self.input, 1024)
            if not chunk:
                break
            reply += chunk
        self.graphics = GRAPHICS_REPLY in reply
        self.reset()
        return self.graphics, self.decoder.feed(reply)

    def size(self):
        rows, cols, xpixel, ypixel = struct.unpack("HHHH", fcntl.ioctl(self.output, termios.TIOCGWINSZ, b"\0" * 8))
        cell = (xpixel // cols, ypixel // rows) if xpixel and ypixel and cols and rows else DEFAULT_CELL
        return cols, rows, cell

    def reset(self):
        self.lines, self.shown = [], {}
        self.write(b"\x1b[2J" + (DELETE_IMAGES if self.graphics else b""))

    def image(self, placement):
        cell_width_pixels, cell_height_pixels = placement.cell
        width = min(MAX_IMAGE[0], placement.cols * cell_width_pixels)
        height = min(MAX_IMAGE[1], placement.rows * cell_height_pixels)
        key = (placement.signature, width, height)
        cached = self.bases.get(placement.chart)
        if cached is None or cached[0] != key:
            radius = max(0.8, cell_height_pixels / 20)
            cached = (key,) + render_chart(placement.chart, placement.points, width, height, radius,
                                           placement.window, placement.maximum)
            self.bases[placement.chart] = cached
        _, canvas, x_of = cached
        if placement.cursor is not None:
            canvas = canvas.copy()
            canvas.vertical(int(x_of(placement.cursor)), REFERENCE, 3, max(1, cell_height_pixels // 20))
        return image_commands(IMAGE_IDS[placement.chart], canvas, placement.cols, placement.rows)

    def present(self, frame):
        covered = {row for image in frame.images for row in range(image.top, image.top + image.rows)}
        rows = [(text, row in covered) for row, text in enumerate(frame.lines)]
        output = [b"\x1b[?2026h"]
        for index, row in enumerate(rows):
            if index >= len(self.lines) or self.lines[index] != row:
                # Preserve images beside padded labels.
                output.append("\x1b[{};1H{}{}".format(index + 1, row[0], "" if row[1] else "\x1b[K").encode("utf-8"))
        self.lines = rows
        wanted = {image.chart: image for image in frame.images}
        for chart in [chart for chart in self.shown if chart not in wanted]:
            output.append("\x1b_Ga=d,d=I,i={},q=2\x1b\\".format(IMAGE_IDS[chart]).encode("ascii"))
            del self.shown[chart]
        for chart, image in wanted.items():
            key = (image.signature, image.cursor, image.top, image.left, image.cols, image.rows, image.cell)
            if self.shown.get(chart) != key:
                output.append("\x1b[{};{}H".format(image.top + 1, image.left + 1).encode("ascii"))
                output.append(self.image(image))
                self.shown[chart] = key
        output.append(b"\x1b[?2026l")
        self.write(b"".join(output))


def inside_multiplexer():
    """Graphics commands become tmux pane titles."""
    return bool(os.environ.get("TMUX")) or os.environ.get("TERM", "").startswith(("screen", "tmux"))


def choose_charts(terminal, text_only):
    if text_only:
        return False, None, []
    if inside_multiplexer():
        return False, "tmux and screen do not pass pixel charts through; showing text charts.", []
    pixels, typed = terminal.detect_graphics()
    if pixels:
        return True, None, typed
    return False, "Pixel charts need Kitty graphics, which this terminal did not report; showing text charts.", typed


def view_loop(state, terminal, dashboard, resized, initial_keys=()):
    seen, refreshed = None, 0
    dirty = True
    keys = initial_keys
    while True:
        now = time.monotonic()
        if dashboard.stale or (state.version != seen and now - refreshed >= REFRESH_SECONDS) \
                or now - refreshed >= HEARTBEAT_SECONDS:
            seen = state.version
            dashboard.update(state.view(dashboard.selected or state.jobs[0].job_id))
            refreshed, dirty = time.monotonic(), True
        for key in keys:
            dirty = True
            if dashboard.key(key):
                return dashboard.exit_status
            if dashboard.stale:
                seen = state.version
                dashboard.update(state.view(dashboard.selected))
                refreshed = time.monotonic()
        if resized[0]:
            resized[0] = False
            terminal.reset()
            dirty = True
        if dashboard.message and now >= dashboard.message[2]:
            dashboard.message, dirty = None, True
        if dirty:
            cols, rows, cell = terminal.size()
            terminal.present(dashboard.compose(cols, rows, cell))
            dirty = False
        data = b""
        if select.select([terminal.input], [], [], terminal.decoder.wait_seconds())[0]:
            try:
                data = os.read(terminal.input, 1024)
            except OSError:
                return 0
            if not data:
                return 0
        keys = terminal.decoder.feed(data)


def run(state, text_only=False, output_dir="downloads"):
    terminal = Terminal()
    resized = [False]

    def on_resize(_signum, _frame):
        resized[0] = True

    def on_stop(signum, _frame):
        raise SystemExit(128 + signum)

    previous = {signal.SIGWINCH: signal.signal(signal.SIGWINCH, on_resize),
                signal.SIGTERM: signal.signal(signal.SIGTERM, on_stop),
                signal.SIGHUP: signal.signal(signal.SIGHUP, on_stop)}
    # Delay tracebacks until terminal restoration.
    errors = io.StringIO()
    try:
        with contextlib.redirect_stderr(errors):
            try:
                terminal.enter()
                pixels, notice, typed = choose_charts(terminal, text_only)
                dashboard = Dashboard(pixels, notice, output_dir)
                return view_loop(state, terminal, dashboard, resized, initial_keys=typed)
            finally:
                terminal.leave()
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
        sys.stderr.write(errors.getvalue())
