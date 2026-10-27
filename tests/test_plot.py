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


    def test_runtime_fallbacks_use_saved_times_and_keep_unknowns_explicit(self):
        for created, source, seconds, display in (
                (None, "samples", 60, "1:00"),
                ("invalid", "samples", 60, "1:00"),
                ("2026-08-31T23:58:00+00:00", "monitoring", 180, "3:00"),
                ("2026-08-30T00:00:00+00:00", "monitoring", 172860, "2-00:01:00"),
                ("2026-09-01T00:00:02+00:00", "monitoring", 58, "0:58"),
                ("2026-09-01T00:03:00+00:00", "monitoring", None, "-")):
            with self.subTest(created=created):
                meta = dict(metadata(), created_at=created)
                result = self.load(entries(meta=meta))
                run = result["runs"][0]
                self.assertEqual(run["metadata"], meta)
                self.assertEqual(run["runtime"], {
                    "source": source, "seconds": seconds, "display": display,
                    "sampled_at": run["nodes"][0]["points"][-1]["time"]})
        result = self.load(entries([sample(0)]))
        self.assertEqual(result["runs"][0]["runtime"]["seconds"], 0)
        empty = jobplot.with_runtime({"runs": [{"metadata": metadata(), "nodes": []}]})
        self.assertIsNone(empty["runs"][0]["runtime"]["seconds"])
        self.assertIsNone(empty["runs"][0]["runtime"]["sampled_at"])

    def test_runtime_uses_the_latest_sample_across_run_nodes(self):
        meta = dict(metadata(), created_at="2026-09-01T00:00:00+00:00",
                    expected_nodes=["node1", "node2"], expected_node_count=2,
                    allocated_cpus_by_node={"node1": 4, "node2": 4})
        files = entries(meta=meta)
        files.append((JOB_ID + "/" + RUN_ID + "/node2--abcdef012345.jsonl",
                      (json.dumps(sample(90, node="node2", stream="node2--abcdef012345")) + "\n").encode()))
        result = self.load(files)
        self.assertEqual(result["runs"][0]["runtime"]["seconds"], 90)
        self.assertEqual(result["runs"][0]["runtime"]["display"], "1:30")
        self.assertIn('<span id="runtime">Elapsed unavailable</span>', jobplot.render_html(result))

    @unittest.skipUnless(shutil.which("node"), "node is needed for the chart rendering regression")
    def test_cpu_chart_marks_full_allocation_and_preserves_usage_above_it(self):
        code = ("function element("
                + jobplot.HTML.split("function element(", 1)[1].split("function draw(){", 1)[0])
        script = self.base / "chart.js"
        script.write_text(code)
        javascript = r"""
const fs=require("node:fs"),vm=require("node:vm"),assert=require("node:assert/strict");
function element(tag){return {tag,attrs:{},children:[],
 setAttribute(key,value){this.attrs[key]=value;},
 appendChild(child){this.children.push(child);},replaceChildren(){this.children=[];}};}
const controls={cpu:element("svg"),memory:element("svg"),axis:{value:"elapsed"}};
const context={byId:id=>controls[id],ns:"http://www.w3.org/2000/svg",
 document:{createElementNS:(_ns,tag)=>element(tag)},series:{
  cpu:[["cpu","Last interval","blue"],["average","Average","orange"],["maximum","Maximum","green"]],
  memory:[["memory","Current","blue"],["memory_peak","Sampled maximum","orange"]]}};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[1],"utf8"),context);
const point=(time,cpu)=>({time,cpu,average:cpu,maximum:cpu,memory:1,memory_peak:1});
const reference=()=>controls.cpu.children.find(child=>child.attrs["data-reference"]==="100-percent");
const labels=()=>controls.cpu.children.filter(child=>child.tag==="text" && child.attrs.x===64)
 .map(child=>child.textContent);
context.chart("cpu",[point(0,50),point(30000,100)]);
assert.deepEqual(labels(),["0%","25%","50%","75%","100%"]);
assert.equal(reference().attrs.y1,16);
context.chart("cpu",[point(0,50),point(30000,160)]);
assert.ok(reference().attrs.y1>16 && reference().attrs.y1<236);
assert.ok(controls.cpu.children.some(child=>child.tag==="text" && child.textContent==="100%"));
const path=controls.cpu.children.find(child=>child.tag==="path").attrs.d;
const coordinates=Array.from(path.matchAll(/[ML]([\d.]+),([\d.]+)/g),match=>Number(match[2]));
assert.ok(coordinates.every(y=>y>=16 && y<=236));
assert.ok(coordinates[1]<reference().attrs.y1,"usage above 100% remains above the reference");
context.chart("cpu",[{...point(0,null),memory:null,memory_peak:null}]);
assert.deepEqual(labels(),["0%","25%","50%","75%","100%"]);
assert.ok(controls.cpu.children.some(child=>child.textContent==="No available measurements"));
context.chart("memory",[point(0,100)]);
assert.ok(!controls.memory.children.some(child=>child.attrs["data-reference"]));
assert.ok(!controls.memory.children.some(child=>String(child.textContent||"").includes("%")));
"""
        result = subprocess.run(["node", "-e", javascript, str(script)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('<h1 id="title">Job history</h1>', jobplot.HTML)
        self.assertNotIn("hpc-live", jobplot.HTML)

    @unittest.skipUnless(shutil.which("node"), "node is needed for the sidebar interaction regression")
    def test_sidebar_toggle_reopens_the_same_controls(self):
        code = ("function toggleSidebar(){"
                + jobplot.HTML.split("function toggleSidebar(){", 1)[1].split("for(const [id,specs]", 1)[0])
        script = self.base / "sidebar.js"
        script.write_text(code)
        javascript = r"""
const fs=require("node:fs"),vm=require("node:vm"),assert=require("node:assert/strict");
const classes=new Set(), attributes={"aria-expanded":"true"};
const sidebar={hidden:false,parentElement:{classList:{toggle(name,enabled){
 if(enabled)classes.add(name);else classes.delete(name);
}}}};
const button={textContent:"",setAttribute:(name,value)=>attributes[name]=value};
const controls={sidebar,"sidebar-toggle":button,run:{value:"1"},node:{value:"2"},axis:{value:"elapsed"}};
const context={byId:id=>controls[id]};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[1],"utf8"),context);
for(let click=0;click<4;click++){
 button.onclick();
 const hidden=click%2===0;
 assert.equal(sidebar.hidden,hidden);
 assert.equal(classes.has("sidebar-collapsed"),hidden);
 assert.equal(attributes["aria-expanded"],String(!hidden));
 assert.equal(button.textContent,"");
 assert.equal(controls.run.value,"1");assert.equal(controls.node.value,"2");
 assert.equal(controls.axis.value,"elapsed");
}
"""
        result = subprocess.run(["node", "-e", javascript, str(script)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('type="button" aria-controls="sidebar" aria-expanded="true"', jobplot.HTML)
        self.assertIn('aria-label="Toggle sidebar" title="Toggle sidebar"', jobplot.HTML)
        self.assertIn('aria-hidden="true" focusable="false"><rect', jobplot.HTML)
        self.assertIn('<aside id="sidebar">', jobplot.HTML)

    @unittest.skipUnless(shutil.which("node"), "node is needed for the job tabs regression")
    def test_job_tabs_preserve_views_live_elapsed_and_selected_job_exports(self):
        meta = dict(metadata(), expected_nodes=["node1", "node2"], expected_node_count=2,
                    allocated_cpus_by_node={"node1": 4, "node2": 4})
        files = entries(meta=meta)
        files.append((JOB_ID + "/" + RUN_ID + "/node2--abcdef012345.jsonl",
                      b"".join((json.dumps(sample(second, node="node2", stream="node2--abcdef012345"))
                                + "\n").encode() for second in (0, 30, 60))))
        second_run = "20260902T000000Z-r1-abcdef012345"
        files += entries([sample(0, run_id=second_run), sample(30, 60, run_id=second_run),
                          sample(60, 120, run_id=second_run)], run_id=second_run)
        first = self.load(files)
        first.update(live=True, connection="Live", version=1)
        for run in first["runs"]:
            run["runtime"].update(source="slurm", seconds=9610, display="2:40:10")
        second = copy.deepcopy(first)
        second.update(job_id="456_2", diagnostics=["second job diagnostic"])
        for run in second["runs"]:
            run["job_name"] = "second simulation α"
            run["metadata"].update(job_id="456_2", job_id_raw="456", job_name=run["job_name"])
            for node in run["nodes"]:
                for point in node["points"]:
                    original = json.loads(point["raw_sample"])
                    original.update(job_id="456_2", job_id_raw="456")
                    point["raw_sample"] = json.dumps(original)
        fixture = self.base / "jobs.json"
        fixture.write_text(json.dumps({"live": True, "jobs": [first, second]}))
        script = self.base / "dashboard.js"
        script.write_text(jobplot.HTML.split('<script>\n"use strict";', 1)[1].split("</script>", 1)[0])
        javascript = r"""
const fs=require("node:fs"),vm=require("node:vm"),assert=require("node:assert/strict");
const elements={},connections=[],listeners={};
class Element{
 constructor(tag){this.tag=tag;this.attrs={};this.children=[];this.dataset={};this.value="";this.textContent="";
  this.hidden=false;this.style={setProperty(){}};this.classList={toggle(){}};}
 set id(value){this._id=value;elements[value]=this;}get id(){return this._id;}
 setAttribute(key,value){this.attrs[key]=value;}removeAttribute(key){delete this.attrs[key];}
 appendChild(child){child.parentElement=this;this.children.push(child);
  if(this.tag==="select" && this.value==="")this.value=String(child.value);}
 replaceChildren(){this.children=[];this.value="";}
 remove(){this.parentElement.children=this.parentElement.children.filter(child=>child!==this);delete elements[this.id];}
 focus(){document.activeElement=this;}
 cloneNode(){return {setAttribute(){},querySelector:()=>null};}
}
for(const id of ["data","run","node","axis","title","runtime","connection","context","scheduler","scope",
 "diagnostics","notice","cpu","memory","cpu-reading","memory-reading","cpu-legend","memory-legend",
 "sidebar","sidebar-toggle","job-tabs","job-panel","export-status"]){
 const item=new Element(["run","node","axis"].includes(id)?"select":"div");item.id=id;
}
elements.sidebar.parentElement=new Element("main");elements.axis.value="clock";
const initial=JSON.parse(fs.readFileSync(process.argv[1],"utf8"));elements.data.textContent=JSON.stringify(initial);
const saves=["cpu","memory"].map(id=>({dataset:{save:id}}));
const document={getElementById:id=>elements[id],createElement:tag=>new Element(tag),
 createElementNS:(_ns,tag)=>new Element(tag),querySelectorAll:()=>saves,
 querySelector:selector=>saves.find(button=>selector.includes('"'+button.dataset.save+'"'))};
const context={document,window:{addEventListener:(type,callback)=>listeners[type]=callback},
 EventSource:class{constructor(){connections.push(this);}close(){this.closed=true;}},
 XMLSerializer:class{serializeToString(){return "<svg/>";}},TextEncoder,Uint8Array,DataView,Blob};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[2],"utf8"),context);
const click=id=>elements["job-tab-"+id].onclick();
assert.equal(connections.length,1);assert.equal(elements["job-tabs"].hidden,false);
assert.equal(elements["job-tabs"].children.length,2);
elements.run.value="0";elements.run.onchange();elements.node.value="1";elements.node.onchange();
elements.axis.value="elapsed";elements.axis.onchange();elements["sidebar-toggle"].onclick();
click("456_2");assert.match(document.title,/456_2 \(second simulation α\)/);
assert.equal(elements.axis.value,"clock");assert.equal(elements.diagnostics.textContent,"second job diagnostic");
const path=elements.cpu.children.find(child=>child.tag==="path");
const next=JSON.parse(JSON.stringify(initial));next.jobs[0].version++;
next.jobs[0].runs[0].runtime.display="2:40:40";
connections[0].onmessage({data:JSON.stringify(next)});
assert.equal(elements.cpu.children.find(child=>child.tag==="path"),path,"background updates preserve the active chart");
click(initial.jobs[0].job_id);
assert.equal(elements.run.value,0);assert.equal(elements.node.value,1);assert.equal(elements.axis.value,"elapsed");
assert.equal(elements.runtime.textContent,"Elapsed 2:40:40");assert.equal(elements.sidebar.hidden,true);
const exported=JSON.parse(context.exportFiles("cpu").files.find(([name])=>name==="data.json")[1]);
assert.equal(exported.job_id,initial.jobs[0].job_id);assert.equal(exported.node,"node2");
assert.equal(exported.run_id,initial.jobs[0].runs[0].run_id);
next.jobs[0].version++;next.jobs[0].runs[0].runtime.display="2:41:10";
connections[0].onmessage({data:JSON.stringify(next)});
assert.equal(elements.runtime.textContent,"Elapsed 2:41:10","elapsed updates without page reload");
elements["job-tab-"+initial.jobs[0].job_id].onkeydown({key:"ArrowRight",preventDefault(){}});
assert.equal(document.activeElement.id,"job-tab-456_2");
assert.equal(Number(elements.run.value),1,"second job keeps its latest run");
assert.equal(Number(elements.node.value),0,"second job keeps its own node");
const other=JSON.parse(context.exportFiles("memory").files.find(([name])=>name==="data.json")[1]);
assert.equal(other.job_id,"456_2");assert.equal(elements.diagnostics.textContent,"second job diagnostic");
connections[0].onmessage({data:JSON.stringify(next.jobs[0])});
assert.equal(elements["job-tabs"].hidden,true,"one job needs no tab bar");
assert.equal(elements["job-panel"].attrs.role,undefined);
assert.equal(connections.length,1);listeners.pagehide();assert.equal(connections[0].closed,true);
"""
        result = subprocess.run(["node", "-e", javascript, str(fixture), str(script)],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

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

