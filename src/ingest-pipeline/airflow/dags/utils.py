import shlex

import yaml
import re
import math
import os
import jsonref
import json
import requests

from airflow import DAG
from urllib import parse as urlparse
from urllib.request import urlopen
from jsonschema import SchemaError, validate, ValidationError
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Pattern,
    Tuple,
    TypeVar,
    Union,
)
from os.path import dirname, join, realpath, relpath, splitext
from subprocess import CalledProcessError, check_output
from pathlib import Path
from collections import namedtuple
from airflow.models.baseoperator import BaseOperator
from airflow.configuration import conf as airflow_conf

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
StrOrListStr = TypeVar("StrOrListStr", str, List[str])
PathStrOrList = Union[str, Path, Iterable[Union[str, Path]]]


PIPELINE_BASE_DIR = Path(__file__).resolve().parent / "cwl"


RESOURCE_MAP_FILENAME = "resource_map.yml"  # Expected to be found in this same dir
RESOURCE_MAP_SCHEMA = "resource_map_schema.yml"
COMPILED_RESOURCE_MAP: Optional[List[Tuple[Pattern, int, Dict[str, Any]]]] = None

WORKFLOW_MAP_FILENAME = "workflow_map.yml"  # Expected to be found in this same dir
WORKFLOW_MAP_SCHEMA = "workflow_map_schema.yml"

GIT = "git"
GIT_LOG_COMMAND = [GIT, "log", "-n1", "--oneline"]
GIT_ORIGIN_COMMAND = [GIT, "config", "--get", "remote.origin.url"]
GIT_ROOT_COMMAND = [GIT, "rev-parse", "--show-toplevel"]
RE_GIT_URL_PATTERN = re.compile(r"(^git@github.com:)(.*)(\.git)")

_SCHEMA_BASE_PATH = join(dirname(dirname(dirname(realpath(__file__)))), "schemata")
_SCHEMA_BASE_URI = "http://schemata.hubmapconsortium.org/"

SequencingDagParameters = namedtuple(
    "SequencingDagParameters",
    [
        "dag_id",
        "pipeline_name",
        "assay",
    ],
)


def set_schema_base_path(base_path: str, base_uri: str):
    if base_path:
        global _SCHEMA_BASE_PATH
        _SCHEMA_BASE_PATH = os.path.abspath(base_path)

    if base_uri:
        global _SCHEMA_BASE_URI
        _SCHEMA_BASE_URI = base_uri


def check_schema_base_path():
    if not _SCHEMA_BASE_PATH:
        raise SchemaError("Make sure to first set _SCHEMA_BASE_PATH (base_path) to the location of the json/yaml "
                          "file to process.")

    if not _SCHEMA_BASE_URI:
        raise SchemaError("Make sure to first set _SCHEMA_BASE_URI (base_uri) to the location of the json/yaml "
                          "file to process, and _SCHEMA_BASE_URI to the corresponding URI.")


class LocalJsonLoader(jsonref.JsonLoader):
    def __init__(self, schema_root_dir, **kwargs):
        super(LocalJsonLoader, self).__init__(**kwargs)
        self.schema_root_dir = schema_root_dir
        self.schema_root_uri = None  # the name by which the root doc knows itself

    def patch_uri(self, uri):
        suri = urlparse.urlsplit(uri)
        if self.schema_root_uri is not None:
            root_suri = urlparse.urlsplit(self.schema_root_uri)
            if suri.scheme == root_suri.scheme and suri.netloc == root_suri.netloc:
                # This file is actually local
                suri = suri._replace(scheme='file', netloc='')
        if suri.scheme == 'file' and not suri.path.startswith(self.schema_root_dir):
            assert suri.path[0] == '/', 'problem parsing path component of a file uri'
            puri = urlparse.urlunsplit((suri.scheme, suri.netloc,
                                        join(self.schema_root_dir, suri.path[1:]),
                                        suri.query, suri.fragment))
        else:
            puri = urlparse.urlunsplit(suri)
        return puri

    def __call__(self, uri, **kwargs):
        rslt = super(LocalJsonLoader, self).__call__(uri, **kwargs)
        if self.schema_root_uri is None and '$id' in rslt:
            self.schema_root_uri = rslt['$id']
            if uri in self.store:
                self.store[self.schema_root_uri] = self.store[uri]
        return rslt

    def get_remote_json(self, uri, **kwargs):
        uri = self.patch_uri(uri)
        puri = urlparse.urlsplit(uri)
        scheme = puri.scheme
        ext = splitext(puri.path)[1]
        other_kwargs = {k: v for k, v in kwargs.items() if k not in ['base_uri', 'jsonschema']}

        if scheme in ["http", "https"]:
            # Prefer requests, it has better encoding detection
            result = requests.get(uri).json(**kwargs)
        else:
            # Otherwise, pass off to urllib and assume utf-8
            if ext in ['.yml', '.yaml']:
                result = yaml.safe_load(urlopen(uri).read().decode("utf-8"), **other_kwargs)
            else:
                result = json.loads(urlopen(uri).read().decode("utf-8"), **other_kwargs)

        return result


def _load_json_schema(filename):
    """
    Loads the schema file of the given name.

    The filename is relative to the root schema directory.
    JSON and YAML formats are supported.
    """
    check_schema_base_path()
    loader = LocalJsonLoader(_SCHEMA_BASE_PATH)
    src_uri = 'file:///{}'.format(filename)
    base_uri = '{}{}'.format(_SCHEMA_BASE_URI, filename)
    return jsonref.load_uri(src_uri, base_uri=base_uri, loader=loader,
                            jsonschema=True, load_on_repr=False)


def assert_json_matches_schema(jsondata, schema_filename: str, base_path: str = "", base_uri: str = ""):
    """
    raises AssertionError if the schema in schema_filename
    is invalid, or if the given jsondata does not match the schema.
    """
    set_schema_base_path(base_path=base_path, base_uri=base_uri)

    schema = _load_json_schema(schema_filename)
    try:
        validate(instance=jsondata, schema=schema)
    except SchemaError as e:
        raise AssertionError('{} is an invalid schema: {}'.format(schema_filename, e))
    except ValidationError as e:
        raise AssertionError('json does not match {}: {}'.format(schema_filename, e))
    else:
        return True


def localized_assert_json_matches_schema(jsn: JSONType, schemafile: str) -> None:
    """
    This version of assert_json_matches_schema knows where to find schemata used by this module
    """
    try:
        return assert_json_matches_schema(jsn, schemafile)  # localized by set_schema_base_path
    except AssertionError as e:
        print("ASSERTION FAILED: {}".format(e))
        raise


def _get_resource_map() -> List[Tuple[Pattern, Pattern, Dict[str, str]]]:
    """
    Lazy compilation of resource map
    """
    global COMPILED_RESOURCE_MAP
    if COMPILED_RESOURCE_MAP is None:
        map_path = join(dirname(__file__), RESOURCE_MAP_FILENAME)
        with open(map_path, "r") as f:
            map = yaml.safe_load(f)
        localized_assert_json_matches_schema(map, RESOURCE_MAP_SCHEMA)
        cmp_map = []
        for dct in map["resource_map"]:
            dag_re = re.compile(dct["dag_re"])
            dag_dct = {key: dct[key] for key in dct if key not in ["dag_re", "tasks"]}
            tasks = []
            for inner_dct in dct["tasks"]:
                assert "task_re" in inner_dct, "schema should guarantee" ' "task_re" is present?'
                task_re = re.compile(inner_dct["task_re"])
                task_dct = {key: inner_dct[key] for key in inner_dct if key not in ["task_re"]}
                tasks.append((task_re, task_dct))
            cmp_map.append((dag_re, dag_dct, tasks))
        COMPILED_RESOURCE_MAP = cmp_map
    return COMPILED_RESOURCE_MAP


def _lookup_resource_record(dag_id: str, task_id: Optional[str] = None) -> Tuple[int, Dict]:
    """
    Look up the resource map entry for the given dag_id and task_id. The first
    match is returned.  If the task_id is None, the first record matching only
    the dag_id is returned and only the information which is not task_id-specific
    is included.
    """
    for dag_re, dag_dict, task_list in _get_resource_map():
        if dag_re.match(dag_id):
            rslt = dag_dict.copy()
            if task_id is not None:
                for task_re, task_dict in task_list:
                    if task_re.match(task_id):
                        rslt.update(task_dict)
                        break
                else:
                    raise ValueError(
                        f"Resource map entry for dag_id <{dag_id}>"
                        f" has no match for task_id <{task_id}>"
                    )
            return rslt
    else:
        raise ValueError(
            "No resource map entry found for" f" dag_id <{dag_id}> task_id <{task_id}>"
        )


def get_lanes_resource(dag_id: str) -> int:
    """
    Look up the number of lanes defined for this dag_id in the current
    resource map.
    """
    rec = _lookup_resource_record(dag_id)
    assert "lanes" in rec, 'schema should guarantee "lanes" is present?'
    return int(rec["lanes"])


def map_queue_name(raw_queue_name: str) -> str:
    """
    If the configuration contains QUEUE_NAME_TEMPLATE, use it to customize the
    provided queue name.  This allows job separation under Celery.
    """
    conf_dict = airflow_conf.as_dict()
    if "QUEUE_NAME_TEMPLATE" in conf_dict.get("connections", {}):
        template = conf_dict["connections"]["QUEUE_NAME_TEMPLATE"]
        template = template.strip("'").strip('"')  # remove quotes that may be on the config string
        rslt = template.format(raw_queue_name)
        return rslt
    else:
        return raw_queue_name


def get_queue_resource(dag_id: str, task_id: Optional[str] = None) -> str:
    """
    Look up the queue defined for this dag_id and task_id in the current
    resource map.  If the task_id is None, the lookup is done with
    task_id='__default__', which presumably only matches the wildcard case.
    """
    if task_id is None:
        task_id = "__default__"
    rec = _lookup_resource_record(dag_id, task_id)
    assert "queue" in rec, 'schema should guarantee "queue" is present?'
    return map_queue_name(rec["queue"])


class HMDAG(DAG):
    """
    A wrapper class for an Airflow DAG which applies certain defaults.
    Defaults are applied to the DAG itself, and to any Tasks added to
    the DAG.
    """

    def __init__(self, dag_id: str, **kwargs):
        """
        Provide "max_active_runs" from the lanes resource, if it is
        not already present.
        """
        if "max_active_runs" not in kwargs:
            kwargs["max_active_runs"] = get_lanes_resource(dag_id)
        super().__init__(dag_id, **kwargs)

    def add_task(self, task: BaseOperator):
        """
        Provide "queue".  This overwrites existing data on the fly
        unless the queue specified in the resource table is None.

        TODO: because a value will be set for "queue" in BaseOperator
        based on conf.get('celery', 'default_queue') it is not easy
        to know if the creator of this task tried to override that
        default value.  One would have to monkeypatch BaseOperator
        to respect a queue specified on the task definition line.
        """
        res_queue = get_queue_resource(self.dag_id, task.task_id)
        if res_queue is not None:
            try:
                task.queue = res_queue
            except Exception as e:
                print(repr(e))
        super().add_task(task)


def _get_scratch_base_path() -> Path:
    dct = airflow_conf.as_dict(display_sensitive=True)["connections"]
    if "WORKFLOW_SCRATCH" in dct:
        scratch_path = dct["WORKFLOW_SCRATCH"]
    elif "workflow_scratch" in dct:
        # support for lower case is necessary setting the scratch path via the
        # environment variable AIRFLOW__CONNECTIONS__WORKFLOW_SCRATCH
        scratch_path = dct["workflow_scratch"]
    else:
        raise KeyError("WORKFLOW_SCRATCH")  # preserve original code behavior
    scratch_path = scratch_path.strip("'").strip('"')  # remove quotes that may be on the string
    return Path(scratch_path)


def get_tmp_dir_path(run_id: str) -> Path:
    """
    Given the run_id, return the path to the dag run's scratch directory
    """
    return _get_scratch_base_path() / run_id


def get_git_commits(file_list: StrOrListStr) -> StrOrListStr:
    """
    Given a list of file paths, return a list of the current short commit hashes of those files
    """
    rslt = []
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
        unroll = True
    else:
        unroll = False
    for fname in file_list:
        log_command = [piece.format(fname=fname) for piece in GIT_LOG_COMMAND]
        try:
            dirnm = dirname(fname)
            if dirnm == "":
                dirnm = "."
            line = check_output(log_command, cwd=dirnm)
        except CalledProcessError as e:
            # Git will fail if this is not running from a git repo
            line = "DeadBeef git call failed: {}".format(e.output)
            line = line.encode("utf-8")
        hashval = line.split()[0].strip().decode("utf-8")
        rslt.append(hashval)
    if unroll:
        return rslt[0]
    else:
        return rslt


def _convert_git_to_proper_url(raw_url: str) -> str:
    """
    If the provided string is of the form git@github.com:something.git, return
    https://github.com/something .  Otherwise just return the input string.
    """
    m = RE_GIT_URL_PATTERN.fullmatch(raw_url)
    if m:
        return f"https://github.com/{m[2]}"
    else:
        return raw_url


def get_git_origins(file_list: StrOrListStr) -> StrOrListStr:
    """
    Given a list of file paths, return a list of the git origins of those files
    """
    rslt = []
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
        unroll = True
    else:
        unroll = False
    for fname in file_list:
        command = [piece.format(fname=fname) for piece in GIT_ORIGIN_COMMAND]
        try:
            dirnm = dirname(fname)
            if dirnm == "":
                dirnm = "."
            line = check_output(command, cwd=dirnm)
        except CalledProcessError as e:
            # Git will fail if this is not running from a git repo
            line = "https://unknown/unknown.git git call failed: {}".format(e.output)
            line = line.encode("utf-8")
        url = line.split()[0].strip().decode("utf-8")
        url = _convert_git_to_proper_url(url)
        rslt.append(url)
    if unroll:
        return rslt[0]
    else:
        return rslt


def get_git_root_paths(file_list: Iterable[str]) -> Union[str, List[str]]:
    """
    Given a list of file paths, return a list of the root directories of the git
    working trees of the files.
    """
    rslt = []
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
        unroll = True
    else:
        unroll = False
    for fname in file_list:
        command = [piece.format(fname=fname) for piece in GIT_ROOT_COMMAND]
        try:
            dirnm = dirname(fname)
            if dirnm == "":
                dirnm = "."
            root_path = check_output(command, cwd=dirnm)
        except CalledProcessError as e:
            print(f"Exception {e}")
            root_path = dirname(fname).encode("utf-8")
        rslt.append(root_path.strip().decode("utf-8"))
    if unroll:
        return rslt[0]
    else:
        return rslt


def get_git_provenance_list(file_list: Iterable[str]) -> List[Mapping[str, Any]]:
    """
    Given a list of file paths, return a list of dicts of the form:

      [{'name':<file base name>, 'hash':<file commit hash>, 'origin':<file git origin>},...]
    """
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
    name_l = file_list
    hash_l = [get_git_commits(realpath(fname)) for fname in file_list]
    origin_l = [get_git_origins(realpath(fname)) for fname in file_list]
    root_l = get_git_root_paths(file_list)
    rel_name_l = [relpath(name, root) for name, root in zip(name_l, root_l)]
    # Make sure each repo appears only once
    repo_d = {
        origin: {"name": name, "hash": hashed}
        for origin, name, hashed in zip(origin_l, rel_name_l, hash_l)
    }
    rslt = []
    for origin in repo_d:
        dct = repo_d[origin].copy()
        dct["origin"] = origin
        if not dct["name"].endswith("cwl"):
            del dct["name"]  # include explicit names for workflows only
        rslt.append(dct)
    # pprint(rslt)
    return rslt


def _get_workflow_map() -> List[Tuple[Pattern, Pattern, str]]:
    """
    Lazy compilation of workflow map
    """
    global COMPILED_WORKFLOW_MAP
    if COMPILED_WORKFLOW_MAP is None:
        map_path = join(dirname(__file__), WORKFLOW_MAP_FILENAME)
        with open(map_path, "r") as f:
            map = yaml.safe_load(f)
        localized_assert_json_matches_schema(map, WORKFLOW_MAP_SCHEMA)
        cmp_map = []
        for dct in map["workflow_map"]:
            ct_re = re.compile(dct["collection_type"])
            at_re = re.compile(dct["assay_type"])
            cmp_map.append((ct_re, at_re, dct["workflow"]))
        COMPILED_WORKFLOW_MAP = cmp_map
    return COMPILED_WORKFLOW_MAP


def get_preserve_scratch_resource(dag_id: str) -> bool:
    """
    Look up the number of lanes defined for this dag_id in the current
    resource map.
    """
    rec = _lookup_resource_record(dag_id)
    assert "preserve_scratch" in rec, "schema should guarantee" ' "preserve_scratch" is present?'
    return bool(rec["preserve_scratch"])


def downstream_workflow_iter(collectiontype: str, assay_type: StrOrListStr) -> Iterable[str]:
    """
    Returns an iterator over zero or more workflow names matching the given
    collectiontype and assay_type.  Each workflow name is expected to correspond to
    a known workflow, e.g. an Airflow DAG implemented by workflow_name.py .
    """
    collectiontype = collectiontype or ""
    assay_type = assay_type or ""
    for ct_re, at_re, workflow in _get_workflow_map():
        if isinstance(assay_type, str):
            at_match = at_re.match(assay_type)
        else:
            at_match = all(at_re.match(elt) for elt in assay_type)
        if ct_re.match(collectiontype) and at_match:
            yield workflow


def pythonop_maybe_keep(**kwargs) -> str:
    """
    accepts the following via the caller's op_kwargs:
    'next_op': the operator to call on success
    'bail_op': the operator to which to bail on failure (default 'no_keep')
    'test_op': the operator providing the success code
    'test_key': xcom key to test.  Defaults to None for return code
    """
    bail_op = kwargs["bail_op"] if "bail_op" in kwargs else "no_keep"
    test_op = kwargs["test_op"]
    test_key = kwargs["test_key"] if "test_key" in kwargs else None
    retcode = int(kwargs["ti"].xcom_pull(task_ids=test_op, key=test_key))
    print("%s key %s: %s\n" % (test_op, test_key, retcode))
    if retcode == 0:
        return kwargs["next_op"]
    else:
        return bail_op


def get_threads_resource(dag_id: str, task_id: Optional[str] = None) -> int:
    """
    Look up the number of threads defined for this dag_id and task_id in
    the current resource map.  If the task_id is None, the lookup is done
    with task_id='__default__', which presumably only matches the wildcard
    case.
    """
    if task_id is None:
        task_id = "__default__"
    rec = _lookup_resource_record(dag_id, task_id)
    assert any(
        ["threads" in rec, "coreuse" in rec]
    ), 'schema should guarantee "threads" or "coreuse" is present?'
    if rec.get("coreuse"):
        return (
            math.ceil(os.cpu_count() * (int(rec.get("coreuse")) / 100))
            if int(rec.get("coreuse")) > 0
            else math.ceil(os.cpu_count() / 4)
        )
    else:
        return int(rec.get("threads"))


def get_absolute_workflows(*workflows: Path) -> List[Path]:
    """
    :param workflows: iterable of `Path`s to CWL files, absolute
      or relative
    :return: Absolute paths to workflows: if the input paths were
      already absolute, they are returned unchanged; if relative,
      they are anchored to `PIPELINE_BASE_DIR`
    """
    return [PIPELINE_BASE_DIR / workflow for workflow in workflows]


def get_parent_dataset_uuids_list(**kwargs) -> List[str]:
    uuid_list = kwargs["dag_run"].conf["parent_submission_id"]
    if not isinstance(uuid_list, list):
        uuid_list = [uuid_list]
    return uuid_list


def build_dataset_name(dag_id: str, pipeline_str: str, **kwargs) -> str:
    parent_submission_str = "_".join(get_parent_dataset_uuids_list(**kwargs))
    return f"{dag_id}__{parent_submission_str}__{pipeline_str}"


def get_parent_dataset_paths_list(**kwargs) -> List[Path]:
    path_list = kwargs["dag_run"].conf["parent_lz_path"]
    if not isinstance(path_list, list):
        path_list = [path_list]
    return [Path(p) for p in path_list]


def get_parent_data_dirs_list(**kwargs) -> List[Path]:
    """
    Build the absolute paths to the data, including the data_path offsets from
    the parent datasets' metadata
    """
    ctx = kwargs["dag_run"].conf
    data_dir_list = get_parent_dataset_paths_list(**kwargs)
    ctx_md_list = ctx["metadata"]
    if not isinstance(ctx_md_list, list):
        ctx_md_list = [ctx_md_list]
    assert len(data_dir_list) == len(
        ctx_md_list
    ), "lengths of data directory and md lists do not match"
    return [
        Path(data_dir) / ctx_md["metadata"]["data_path"]
        for data_dir, ctx_md in zip(data_dir_list, ctx_md_list)
    ]


def get_cwltool_base_cmd(tmpdir: Path) -> List[str]:
    return [
        "env",
        "TMPDIR={}".format(tmpdir),
        "_JAVA_OPTIONS={}".format("-XX:ActiveProcessorCount=2"),
        "cwltool",
        "--timestamps",
        "--preserve-environment",
        "_JAVA_OPTIONS",
        # The trailing slashes in the next two lines are deliberate.
        # cwltool treats these path prefixes as *strings*, not as
        # directories in which new temporary dirs should be created, so
        # a path prefix of '/tmp/cwl-tmp' will cause cwltool to use paths
        # like '/tmp/cwl-tmpXXXXXXXX' with 'XXXXXXXX' as a random string.
        # Adding the trailing slash is ensures that temporary directories
        # are created as *subdirectories* of 'cwl-tmp' and 'cwl-out-tmp'.
        "--tmpdir-prefix={}/".format(tmpdir / "cwl-tmp"),
        "--tmp-outdir-prefix={}/".format(tmpdir / "cwl-out-tmp"),
    ]


def join_quote_command_str(pieces: List[Any]):
    command_str = " ".join(shlex.quote(str(piece)) for piece in pieces)
    print("final command_str:", command_str)
    return command_str
