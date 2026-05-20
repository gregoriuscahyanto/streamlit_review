import re
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def _src() -> str:
    # UTF-8 source read to avoid encoding artifacts.
    return APP_PATH.read_text(encoding="utf-8")


def test_claim_allows_open_and_reviewed_cases():
    s = _src()
    assert "status in ('open', 'reviewed')" in s


def test_claim_blocks_same_reviewer_twice():
    s = _src()
    p = r"and rl\.reviewer = :reviewer"
    assert re.search(p, s), "Reviewer filter missing in claim query."


def test_release_restores_reviewed_when_labels_exist():
    s = _src()
    assert "then 'reviewed'" in s
    assert "else 'open'" in s


def test_fetch_runs_includes_reviewed_cases():
    s = _src()
    assert "where c.status in ('open', 'in_review', 'reviewed')" in s


def test_sidebar_uses_claimable_count_for_reviewer():
    s = _src()
    assert "def fetch_global_claimable_count(reviewer: str)" in s
    assert "status in ('open', 'reviewed')" in s
    assert "and rl.reviewer = :reviewer" in s
    assert "Fuer dich verfuegbar insgesamt" in s
