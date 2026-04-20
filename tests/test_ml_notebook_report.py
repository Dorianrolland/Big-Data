from __future__ import annotations

import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = ROOT / "ml" / "notebooks" / "copilot_training_report.ipynb"


def _notebook_payload() -> dict:
    return json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))


def test_training_report_notebook_exists_and_is_valid_json():
    assert NOTEBOOK_PATH.exists(), "missing notebook ml/notebooks/copilot_training_report.ipynb"
    payload = _notebook_payload()
    assert payload.get("nbformat") == 4
    assert isinstance(payload.get("cells"), list)
    assert payload["cells"], "notebook has no cells"


def test_training_report_notebook_contains_required_pipeline_sections():
    payload = _notebook_payload()
    text = "\n".join(
        "".join(cell.get("source", []))
        for cell in payload.get("cells", [])
        if isinstance(cell, dict)
    )
    required_snippets = [
        "build_training_frame(",
        "train_with_evaluation(",
        "roc_curve(",
        "precision_recall_curve(",
        "calibration_curve(",
        "random_split_roc_auc",
        "quality_flags",
        "joblib.dump(",
        "copilot_training_report.json",
        "copilot_training_report.md",
        "copilot_feature_importance.png",
    ]
    missing = [snippet for snippet in required_snippets if snippet not in text]
    assert not missing, f"notebook missing required snippets: {missing}"


def test_training_report_notebook_code_cells_are_syntax_valid():
    payload = _notebook_payload()
    code_cells = [
        "".join(cell.get("source", []))
        for cell in payload.get("cells", [])
        if isinstance(cell, dict) and cell.get("cell_type") == "code"
    ]
    assert code_cells, "expected code cells in training report notebook"
    for idx, source in enumerate(code_cells, start=1):
        ast.parse(source, filename=f"copilot_training_report.ipynb#cell{idx}")


def test_training_report_notebook_is_committed_without_execution_output():
    payload = _notebook_payload()
    for cell in payload.get("cells", []):
        if not isinstance(cell, dict):
            continue
        if cell.get("cell_type") != "code":
            continue
        outputs = cell.get("outputs", [])
        assert outputs == [], "notebook should be committed without execution outputs"
