from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def _src() -> str:
    # UTF-8 lesen, um Encoding-Probleme zu vermeiden.
    return APP_PATH.read_text(encoding="utf-8")


def test_save_case_decision_handles_sqlalchemy_error():
    s = _src()
    assert "except SQLAlchemyError as ex:" in s
    assert "write_error_log(" in s
    assert "save_case_decision failed run_id=" in s
    assert "return False" in s


def test_error_log_written_as_utf8():
    s = _src()
    assert "with p.open(\"a\", encoding=\"utf-8\") as f:" in s


def test_save_case_decision_uses_upsert_for_unique_run_pair():
    s = _src()
    assert "on conflict (run_id, pair_key, reviewer)" in s
    assert "do update set" in s


def test_batch_deadline_uses_local_timezone_format():
    s = _src()
    assert "def format_local_hhmm(dt_value):" in s
    assert "normalize_to_utc(dt_value).astimezone().strftime(\"%H:%M\")" in s
    assert "format_local_hhmm(expires_at)" in s
