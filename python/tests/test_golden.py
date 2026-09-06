from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from durable_session import Checkpoint, load
from durable_session.checkpoint import CheckpointError

REPO = Path(__file__).resolve().parents[2]
V0 = REPO / "fixtures" / "v0"
SCHEMA = REPO / "spec" / "schema"


def _schema(name: str) -> dict:
    return json.loads((SCHEMA / name).read_text(encoding="utf-8"))


def test_golden_tools_view() -> None:
    cp = load(V0 / "tools")
    expected = json.loads((V0 / "tools.view.json").read_text(encoding="utf-8"))
    assert cp.session_id == "sess_demo"
    assert cp.view() == expected
    assert Checkpoint.load(V0 / "tools").view() == expected


def test_golden_matches_schemas() -> None:
    tools = V0 / "tools"
    Draft202012Validator(_schema("manifest.schema.json")).validate(
        json.loads((tools / "manifest.json").read_text(encoding="utf-8"))
    )
    event_validator = Draft202012Validator(_schema("event.schema.json"))
    for line in (tools / "transcript.ndjson").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        event_validator.validate(json.loads(line))
    tree_validator = Draft202012Validator(_schema("tree.schema.json"))
    trees = list((tools / "trees").glob("*.json"))
    assert trees
    for path in trees:
        tree_validator.validate(json.loads(path.read_text(encoding="utf-8")))
    view_validator = Draft202012Validator(_schema("view.schema.json"))
    expected = json.loads((V0 / "tools.view.json").read_text(encoding="utf-8"))
    view_validator.validate(expected)
    view_validator.validate(load(tools).view())


def test_reject_seq_gap() -> None:
    with pytest.raises(CheckpointError, match="seq gap"):
        load(V0 / "invalid-seq-gap")


def test_reject_unsupported_version() -> None:
    with pytest.raises(CheckpointError, match="checkpoint_version"):
        load(V0 / "invalid-version")


def test_reject_path_escape() -> None:
    with pytest.raises(CheckpointError, match="escapes"):
        load(V0 / "invalid-escape")


def test_reject_two_pending() -> None:
    with pytest.raises(CheckpointError, match="pending"):
        load(V0 / "invalid-two-pending")


def test_reject_workspace_head_mismatch() -> None:
    with pytest.raises(CheckpointError, match="workspace_head"):
        load(V0 / "invalid-head-mismatch")
