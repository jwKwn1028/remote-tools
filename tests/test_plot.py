"""Offline plotting, archive validation, and workstation fetch regressions."""

import base64
import copy
import csv
import datetime as dt
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobmon"))

import plot as jobplot
from slurm_backend import JobmonError

JOB_ID = "123_7"
RUN_ID = "20260901T000000Z-r0-abcdef012345"
STREAM = "node1--abcdef012345"


def metadata(run_id=RUN_ID, node="node1"):
    return {"schema_version": 1, "job_id": JOB_ID, "job_id_raw": "123", "run_id": run_id,
            "job_name": "synthetic simulation", "uid": 987654, "user": "synthetic-cluster-user",
            "expected_nodes": [node], "expected_node_count": 1, "allocated_cpus_by_node": {node: 4},
            "errors": []}


def sample(second, delta=None, duration=30, memory=1024 ** 3, stream=STREAM, run_id=RUN_ID, node="node1"):
    return {"schema_version": 1, "job_id": JOB_ID, "job_id_raw": "123", "run_id": run_id,
            "node": node, "stream_id": stream, "timestamp": (
                dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=second)).isoformat(),
            "monotonic_seconds": second + 100, "interval_seconds": 30, "final": False,
            "metrics": {"scope": "process_tree_pgid", "allocated_cpus": 4, "cpu_delta_seconds": delta,
                        "cpu_interval_seconds": duration, "memory_bytes": memory, "memory_peak_bytes": None},
            "gpus": [], "errors": []}


def entries(records=None, meta=None, run_id=RUN_ID):
    if records is None:
        records = [sample(0), sample(30, 60), sample(60, 120)]
    return [(JOB_ID + "/" + run_id + "/metadata.json", json.dumps(meta or metadata(run_id)).encode()),
            (JOB_ID + "/" + run_id + "/" + STREAM + ".jsonl",
             b"".join(json.dumps(row).encode() + b"\n" for row in records))]


def archive(path, files):
    with tarfile.open(path, "w:gz") as output:
        for name, contents in files:
            info = tarfile.TarInfo(name)
            if isinstance(contents, tarfile.TarInfo):
                contents.name = name
                output.addfile(contents)
            else:
                info.size = len(contents)
                output.addfile(info, io.BytesIO(contents))
    path.chmod(0o600)
    return path


class PlotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.path = self.base / "jobmon-test.tar.gz"

    def load(self, files=None):
        return jobplot.read_archive(JOB_ID, archive(self.path, entries() if files is None else files))

    def test_timeseries_matches_watch_averages_and_retains_unknowns(self):
        rows = [sample(0), sample(30, 60, memory=2 * 1024 ** 3),
                sample(60, 120, memory=512 * 1024 ** 2), sample(180, 0, duration=120, memory=None)]
        result = self.load(entries(rows))
        node = result["runs"][0]["nodes"][0]
        points = node["points"]
        self.assertEqual(node["scopes"], ["process_tree_pgid"])
        self.assertIsNone(points[0]["cpu"])
        self.assertIsNone(points[0]["average"])
        self.assertEqual([point["cpu"] for point in points], [None, 50, 100, None])
        self.assertEqual([point["average"] for point in points], [None, 50, 75, 75])
        self.assertEqual([point["maximum"] for point in points], [None, 50, 100, 100])
        self.assertEqual([point["memory"] for point in points], [1, 2, 0.5, None])
        self.assertEqual([point["memory_peak"] for point in points], [1, 2, 2, 2])
        self.assertEqual([point["gap"] for point in points], [True, False, False, True])
        self.assertEqual(json.loads(points[0]["raw_sample"]), rows[0])
        self.assertEqual(result["runs"][0]["metadata"], metadata())


    @unittest.skipUnless(shutil.which("node"), "node is needed for the browser export regression")
    def test_save_buttons_bundle_matching_image_values_metadata_and_original_samples(self):
        second = "20260902T000000Z-r1-abcdef012345"
        meta = metadata(second)
        meta.update(job_name="synthetic simulation α", expected_nodes=["node1", "node2"], expected_node_count=2,
                    allocated_cpus_by_node={"node1": 4, "node2": 4})
        rows = [sample(0, run_id=second, node="node2", stream="node2--abcdef012345"),
                sample(30, 0, memory=None, run_id=second, node="node2", stream="node2--abcdef012345"),
                sample(60, 60, run_id=second, node="node2", stream="node2--abcdef012345")]
        rows[0]["large_counter"] = 2 ** 60 + 1
        files = entries() + entries([sample(0, run_id=second)], meta=meta, run_id=second)
        files.append((JOB_ID + "/" + second + "/node2--abcdef012345.jsonl",
                      b"".join(json.dumps(row).encode() + b"\n" for row in rows)))
        fixture = self.base / "fixture.json"
        fixture.write_text(json.dumps(self.load(files)))
        export_script = self.base / "export.js"
        # Exercise the actual click handler and ZIP writer without requiring a GUI.
        code = ("function crc32(bytes){"
                + jobplot.HTML.split("function crc32(bytes){", 1)[1].split("update(data);", 1)[0])
        export_script.write_text(code)
        javascript = r"""
const fs=require("node:fs"),vm=require("node:vm");
const data=JSON.parse(fs.readFileSync(process.argv[1],"utf8")), downloads=[],blobs=[];
const controls={run:{value:"1"},node:{value:"1"},axis:{value:"elapsed"},"export-status":{textContent:""}};
for(const id of ["cpu","memory"])controls[id]={
 cloneNode:()=>({id,setAttribute(){},querySelector:()=>null})
};
const buttons=[{dataset:{save:"cpu"}},{dataset:{save:"memory"}}];
const document={
 querySelectorAll:()=>buttons,
 createElement:()=>({click(){downloads.push({filename:this.download,blob:blobs[Number(this.href)]});}})
};
const context={data,byId:id=>controls[id],ns:"http://www.w3.org/2000/svg",document,Blob,TextEncoder,DataView,
 Uint8Array,URL:{createObjectURL:blob=>(blobs.push(blob)-1).toString(),revokeObjectURL(){}},
 XMLSerializer:class{serializeToString(svg){return '<svg xmlns="http://www.w3.org/2000/svg"><title>'+
  svg.id+' · α</title></svg>';}},setTimeout:fn=>fn()};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[2],"utf8"),context);
buttons[0].onclick();
const points=data.runs[1].nodes[1].points;
points.push({...points[points.length-1],time:points[points.length-1].time+30000});
buttons[1].onclick();
for(const point of points)delete point.raw_sample;
buttons[0].onclick();
const names=["check_df_eri",'../../bad\\name:with?*<>|" spaces\u0000',null,"", "...///","가".repeat(200)];
const filenames=names.map(name=>{
 data.runs[1].job_name=name;
 return vm.runInContext('exportFiles("cpu").filename',context);
});
Promise.all(downloads.map(async result=>({filename:result.filename,
 bytes:Buffer.from(await result.blob.arrayBuffer()).toString("base64")})))
 .then(result=>process.stdout.write(JSON.stringify({downloads:result,filenames})));
"""
        result = subprocess.run(["node", "-e", javascript, str(fixture), str(export_script)],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        exported = json.loads(result.stdout)
        downloads = exported["downloads"]
        safe_names = ["check_df_eri", "bad-name-with-spaces", "unnamed", "unnamed", "unnamed", "가" * 21]
        for filename, name in zip(exported["filenames"], safe_names):
            self.assertTrue(filename.startswith("jobmon-" + JOB_ID + "-" + name + "-" + second + "-node2-cpu-"))
            self.assertLessEqual(len(filename.encode("utf-8")), 240)
            self.assertNotRegex(filename, r'[\\/:*?"<>|\x00-\x1f]')
        self.assertEqual(len(downloads), 3)
        for index, download in enumerate(downloads):
            with self.subTest(index=index), zipfile.ZipFile(io.BytesIO(base64.b64decode(download["bytes"]))) as package:
                expected = {"chart.svg", "data.csv", "data.json"}
                if index < 2:
                    expected.add("samples.jsonl")
                self.assertEqual(set(package.namelist()), expected)
                self.assertIsNone(package.testzip())
                self.assertTrue(all(info.external_attr >> 16 & 0o777 == 0o600 for info in package.infolist()))
                saved = json.loads(package.read("data.json"))
                chart = "memory" if index == 1 else "cpu"
                self.assertEqual(saved["chart"], chart)
                self.assertEqual(saved["run_id"], second)
                self.assertEqual(saved["node"], "node2")
                self.assertEqual(saved["run_metadata"], meta)
                self.assertEqual(saved["units"]["memory"], "GiB")
                self.assertEqual(saved["time_axis"], "elapsed")
                self.assertEqual(len(saved["points"]), 3 if index == 0 else 4)
                prefix = "jobmon-" + JOB_ID + "-synthetic-simulation-α-" + second + "-node2-" + chart
                self.assertTrue(download["filename"].startswith(prefix))
                self.assertIn(chart + " · α", package.read("chart.svg").decode())
                table = list(csv.DictReader(io.StringIO(package.read("data.csv").decode())))
                self.assertEqual(len(table), len(saved["points"]))
                self.assertEqual(table[1]["cpu_percent"], "0")
                self.assertEqual(table[1]["memory_gib"], "")
                self.assertEqual(table[0]["elapsed_seconds"], "0")
                self.assertEqual(saved["raw_samples_complete"], index < 2)
                if index < 2:
                    originals = [json.loads(line) for line in package.read("samples.jsonl").splitlines()]
                    self.assertEqual(originals[:3], rows)
                    self.assertEqual(originals[0]["large_counter"], 2 ** 60 + 1)

    def test_runs_nodes_and_collector_restarts_stay_distinct(self):
        meta = metadata()
        meta["expected_nodes"].append("node2")
        meta["expected_node_count"] = 2
        meta["allocated_cpus_by_node"]["node2"] = 4
        files = entries(meta=meta)
        prefix = JOB_ID + "/" + RUN_ID + "/"
        restart = "node1--bbbbbbbbbbbb"
        files.append((prefix + restart + ".jsonl",
                      (json.dumps(sample(90, stream=restart)) + "\n").encode()))
        files.append((prefix + "node2--abcdef012345.jsonl",
                      (json.dumps(sample(0, node="node2", stream="node2--abcdef012345")) + "\n").encode()))
        second = "20260902T000000Z-r1-abcdef012345"
        files += entries([sample(0, run_id=second)], run_id=second)
        result = self.load(files)
        self.assertEqual(len(result["runs"]), 2)
        self.assertEqual([node["node"] for node in result["runs"][0]["nodes"]], ["node1", "node2"])
        points = result["runs"][0]["nodes"][0]["points"]
        self.assertTrue(points[-1]["gap"])
        self.assertIsNone(points[-1]["cpu"])
        self.assertIsNone(result["runs"][1]["nodes"][0]["points"][0]["average"])

    def test_malformed_and_partial_samples_do_not_erase_valid_history(self):
        files = entries()
        wrong = sample(90, 60)
        wrong["job_id"] = "999"
        files[1] = (files[1][0], files[1][1] + b"not json\n" +
                    json.dumps(wrong).encode() + b"\n" + b'{"timestamp":')
        result = self.load(files)
        self.assertEqual(len(result["runs"][0]["nodes"][0]["points"]), 3)
        diagnostics = "\n".join(result["diagnostics"])
        self.assertIn("identity mismatch", diagnostics)
        self.assertIn("unfinished final line", diagnostics)
        self.assertIn("Expecting value", diagnostics)

    def test_cluster_identity_is_preserved_and_html_values_cannot_inject_markup(self):
        meta = metadata()
        meta["job_name"] = '</script><script>alert("synthetic")</script>&'
        before = copy.deepcopy(meta)
        result = self.load(entries(meta=meta))
        html = jobplot.render_html(result)
        self.assertNotIn(meta["job_name"], html)
        payload = html.split('id="data">', 1)[1].split("</script>", 1)[0]
        self.assertEqual(json.loads(payload)["runs"][0]["job_name"], meta["job_name"])
        self.assertEqual(meta, before)
        self.assertIn("process_tree_pgid", html)
        self.assertIn("All assets stay local", html)

    def test_missing_node_layout_and_malformed_completion_remain_visible(self):
        meta = metadata()
        meta["expected_nodes"] = []
        meta["expected_node_count"] = 2
        completion = {"job_id": "999", "run_id": RUN_ID, "workload_exit_code": 0}
        result = self.load(entries(meta=meta) + [
            (JOB_ID + "/" + RUN_ID + "/completion.json", json.dumps(completion).encode())])
        self.assertIsNone(result["runs"][0]["exit_code"])
        self.assertIn("Incomplete expected-node layout", "\n".join(result["diagnostics"]))
        self.assertIn("invalid completion", "\n".join(result["diagnostics"]))

    def test_valid_completion_and_final_samples_are_retained(self):
        row = sample(30, 60)
        row["final"] = True
        completion = {"job_id": JOB_ID, "run_id": RUN_ID, "workload_exit_code": 1}
        result = self.load(entries([sample(0), row]) + [
            (JOB_ID + "/" + RUN_ID + "/completion.json", json.dumps(completion).encode())])
        self.assertEqual(result["runs"][0]["exit_code"], 1)
        self.assertTrue(result["runs"][0]["nodes"][0]["points"][-1]["final"])

    def test_unsafe_foreign_duplicate_and_link_members_are_rejected_without_extraction(self):
        link = tarfile.TarInfo()
        link.type, link.linkname = tarfile.SYMTYPE, "../outside"
        hardlink = tarfile.TarInfo()
        hardlink.type, hardlink.linkname = tarfile.LNKTYPE, "outside"
        for files in (
                [("../outside", b"bad")], [("/outside", b"bad")], [("999/run/metadata.json", b"{}")],
                [(JOB_ID + "/../metadata.json", b"{}")], entries() + [entries()[0]],
                [(JOB_ID + "/" + RUN_ID + "/metadata.json", link)],
                [(JOB_ID + "/" + RUN_ID + "/metadata.json", hardlink)]):
            with self.subTest(files=[name for name, _ in files]), self.assertRaises(JobmonError):
                self.load(files)
        self.assertFalse((self.base / "outside").exists())

    def test_archive_limits_and_private_file_requirements(self):
        archive(self.path, entries())
        with patch.object(jobplot, "MAX_ARCHIVE_BYTES", 10), self.assertRaises(JobmonError):
            jobplot.read_archive(JOB_ID, self.path)
        with patch.object(jobplot, "MAX_MEMBERS", 1), self.assertRaises(JobmonError):
            jobplot.read_archive(JOB_ID, self.path)
        self.path.chmod(0o644)
        with self.assertRaises(JobmonError):
            jobplot.read_archive(JOB_ID, self.path)
        self.path.chmod(0o600)
        link = self.base / "linked.tar.gz"
        link.symlink_to(self.path)
        with self.assertRaises(OSError):
            jobplot.read_archive(JOB_ID, link)
        with self.assertRaises(JobmonError):
            jobplot.read_archive("../outside", self.path)

    def test_missing_or_invalid_metadata_and_empty_samples_fail_cleanly(self):
        for files in (entries(records=[]), [entries()[1]], entries(meta={"schema_version": 99}),
                      [(entries()[0][0], b"{malformed"), entries()[1]]):
            with self.subTest(files=files), self.assertRaises(JobmonError):
                self.load(files)

    def test_cli_writes_private_html_and_refuses_to_overwrite(self):
        archive(self.path, entries())
        script = [sys.executable, str(ROOT / "jobmon/plot.py"), JOB_ID, str(self.path)]
        result = subprocess.run(script, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = Path(result.stdout.strip())
        self.assertEqual(output.name, "jobmon-test.html")
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        contents = output.read_bytes()
        second = subprocess.run(script, capture_output=True, text=True, timeout=10)
        self.assertEqual(second.returncode, 1)
        self.assertNotIn("Traceback", second.stderr)
        self.assertEqual(output.read_bytes(), contents)

    def test_cli_rejects_invalid_ids_before_opening_an_archive(self):
        result = subprocess.run([sys.executable, str(ROOT / "jobmon/plot.py"), "../outside", str(self.path)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertIn("allocation ID", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()

