import json
import os
import jsonref
from typing import List, Dict
from pathlib import Path
from csv import DictReader
from urllib import parse as urlparse
from os.path import join, dirname, realpath
from jsonschema import SchemaError, validate, ValidationError

from airflow.providers.http.hooks.http import HttpHook
from pprint import pprint
from typing import Tuple

_SCHEMA_BASE_PATH = join(dirname(dirname(dirname(realpath(__file__)))), "schemata")
_SCHEMA_BASE_URI = "http://schemata.hubmapconsortium.org/"


def check_link_published_drvs(uuid: str, auth_tok: str) -> Tuple[bool, str]:
    needs_previous_version = False
    published_uuid = ""
    endpoint = f"/children/{uuid}"
    headers = {
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
        "Authorization": f"Bearer {auth_tok}"
    }
    extra_options = {}

    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    response = http_hook.run(endpoint, headers=headers, extra_options=extra_options)
    print("response: ")
    pprint(response.json())
    for data in response.json():
        if (
            data.get("entity_type") in ("Dataset", "Publication")
            and data.get("status") == "Published"
        ):
            needs_previous_version = True
            published_uuid = data.get("uuid")
    return needs_previous_version, published_uuid


def get_component_uuids(uuid:str, auth_tok: str) -> List:
    children = []
    endpoint = f"/children/{uuid}"
    headers = {
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
        "Authorization": f"Bearer {auth_tok}"
    }
    extra_options = {}

    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    response = http_hook.run(endpoint, headers=headers, extra_options=extra_options)
    print("response: ")
    pprint(response.json())
    for data in response.json():
        if data.get("creation_action") == "Multi-Assay Split":
            children.append(data.get("uuid"))
    return children


class SoftAssayClient:
    def __init__(self, metadata_files: List, auth_tok: str):
        self.assay_components = []
        self.primary_assay = {}
        self.is_multiassay = True
        for metadata_file in metadata_files:
            try:
                rows = self.__read_rows(metadata_file, encoding="UTF-8")
            except Exception as e:
                print(f"Error {e} reading metadata {metadata_file}")
                return
            assay_type = self.__get_assaytype_data(row=rows[0], auth_tok=auth_tok)
            data_component = {
                "assaytype": assay_type.get("assaytype"),
                "dataset-type": assay_type.get("dataset-type"),
                "contains-pii": assay_type.get("contains-pii", True),
                "primary": assay_type.get("primary", False),
                "metadata-file": metadata_file,
            }
            if not assay_type.get("must-contain"):
                print(f"Component {assay_type}")
                self.assay_components.append(data_component)
            else:
                print(f"Primary {assay_type}")
                self.primary_assay = data_component
        if not self.primary_assay and len(self.assay_components) == 1:
            self.primary_assay = self.assay_components.pop()
            self.is_multiassay = False

    def __get_assaytype_data(
        self,
        row: Dict,
        auth_tok: str,
    ) -> Dict:
        http_hook = HttpHook("POST", http_conn_id="ingest_api_connection")
        endpoint = f"/assaytype"
        headers = {
            "Authorization": f"Bearer {auth_tok}",
            "Content-Type": "application/json",
        }
        response = http_hook.run(endpoint=endpoint, headers=headers, data=json.dumps(row))
        response.raise_for_status()
        return response.json()

    def __get_context_of_decode_error(self, e: UnicodeDecodeError) -> str:
        buffer = 20
        codec = "latin-1"  # This is not the actual codec of the string!
        before = e.object[max(e.start - buffer, 0) : max(e.start, 0)].decode(codec)  # noqa
        problem = e.object[e.start : e.end].decode(codec)  # noqa
        after = e.object[e.end : min(e.end + buffer, len(e.object))].decode(codec)  # noqa
        in_context = f"{before} [ {problem} ] {after}"
        return f'Invalid {e.encoding} because {e.reason}: "{in_context}"'

    def __dict_reader_wrapper(self, path, encoding: str) -> list:
        with open(path, encoding=encoding) as f:
            rows = list(DictReader(f, dialect="excel-tab"))
        return rows

    def __read_rows(self, path: Path, encoding: str) -> List:
        if not Path(path).exists():
            message = {"File does not exist": f"{path}"}
            raise message
        try:
            rows = self.__dict_reader_wrapper(path, encoding)
            if not rows:
                message = {"File has no data rows": f"{path}"}
            else:
                return rows
        except IsADirectoryError:
            message = {"Expected a TSV, but found a directory": f"{path}"}
        except UnicodeDecodeError as e:
            message = {"Decode Error": self.__get_context_of_decode_error(e)}
        raise message


def set_schema_base_path(base_path: str, base_uri: str):
    if base_path:
        global _SCHEMA_BASE_PATH
        _SCHEMA_BASE_PATH = os.path.abspath(base_path)

    if base_uri:
        global _SCHEMA_BASE_URI
        _SCHEMA_BASE_URI = base_uri


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


def check_schema_base_path():
    if not _SCHEMA_BASE_PATH:
        raise SchemaError("Make sure to first set _SCHEMA_BASE_PATH (base_path) to the location of the json/yaml "
                          "file to process.")

    if not _SCHEMA_BASE_URI:
        raise SchemaError("Make sure to first set _SCHEMA_BASE_URI (base_uri) to the location of the json/yaml "
                          "file to process, and _SCHEMA_BASE_URI to the corresponding URI.")


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
