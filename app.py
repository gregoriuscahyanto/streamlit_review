import html
import json
import re
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Blocking Review", layout="wide")

DECISION_OPTIONS = ["BLOCK_OK", "BLOCK_NOK", "UNSURE"]
CLAIM_TIMEOUT_MINUTES = 30
REQUIRED_LOCK_COLUMNS = {"locked_by", "locked_at"}
DEFAULT_BATCH_SIZE = 20
MAX_BACK_HISTORY = 5


@st.cache_resource
def get_engine(db_url: str):
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        future=True,
    )


DB_URL = st.secrets.get("DB_URL")
if not DB_URL:
    st.error("Secret DB_URL fehlt.")
    st.stop()

engine = get_engine(DB_URL)


# =========================================================
# PAGE CSS
# =========================================================
def apply_css(mobile_mode: bool):
    if mobile_mode:
        st.markdown(
            """
            <style>
            [data-testid="stHeader"] {
                height: 3rem !important;
                min-height: 3rem !important;
            }
            .block-container {
                padding-top: 0.9rem !important;
                padding-bottom: 1rem !important;
                max-width: 100% !important;
            }
            [data-testid="stSidebar"] { overflow-y: auto !important; }
            .app-main-title {
                display:block;
                font-size:2.2rem;
                font-weight:800;
                line-height:1.2;
                margin:0.1rem 0 1rem 0;
                padding-top:0.2rem;
            }
            .review-section-title {
                font-size:18px;
                font-weight:700;
                margin-top:6px;
                margin-bottom:8px;
            }
            .top-score-card {
                border:1px solid #dfe3e8;
                border-radius:12px;
                padding:14px 12px;
                background:#f8f9fa;
                margin-bottom:10px;
                text-align:center;
                min-height:190px;
                display:flex;
                flex-direction:column;
                justify-content:center;
            }
            .top-score-label { font-size:12px; color:#6c757d; margin-bottom:4px; }
            .top-score-value { font-size:24px; font-weight:800; line-height:1.1; margin-bottom:10px; }
            .compact-note { font-size:13px; color:#6c757d; }
            .reviewer-required-box {
                border: 1px solid #e0e0e0;
                border-radius: 12px;
                padding: 18px;
                background: #fafafa;
                margin-top: 14px;
                margin-bottom: 12px;
            }
            .sidebar-stat-title {
                font-size: 13px;
                font-weight: 700;
                margin-bottom: 4px;
            }
            .sidebar-stat-value {
                font-size: 13px;
                font-weight: 700;
                text-align: right;
                white-space: nowrap;
                padding-top: 1px;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <style>
            [data-testid="stHeader"] {
                height: 3.2rem !important;
                min-height: 3.2rem !important;
            }
            .block-container {
                padding-top: 1rem !important;
                padding-bottom: 0.6rem !important;
                max-width: 100% !important;
            }
            [data-testid="stSidebar"] { overflow-y: auto !important; }
            .app-main-title {
                display:block;
                font-size:2.7rem;
                font-weight:800;
                line-height:1.18;
                margin:0.1rem 0 1rem 0;
                padding-top:0.15rem;
            }
            .review-section-title {
                font-size:18px;
                font-weight:700;
                margin-top:6px;
                margin-bottom:8px;
            }
            .top-score-card {
                border:1px solid #dfe3e8;
                border-radius:12px;
                padding:18px 16px;
                background:#f8f9fa;
                margin-bottom:10px;
                text-align:center;
                min-height:190px;
                display:flex;
                flex-direction:column;
                justify-content:center;
            }
            .top-score-label { font-size:14px; color:#6c757d; margin-bottom:4px; }
            .top-score-value { font-size:34px; font-weight:800; line-height:1.1; margin-bottom:14px; }
            .compact-note { font-size:13px; color:#6c757d; }
            .reviewer-required-box {
                border: 1px solid #e0e0e0;
                border-radius: 12px;
                padding: 22px;
                background: #fafafa;
                margin-top: 14px;
                margin-bottom: 14px;
            }
            .sidebar-stat-title {
                font-size: 13px;
                font-weight: 700;
                margin-bottom: 4px;
            }
            .sidebar-stat-value {
                font-size: 13px;
                font-weight: 700;
                text-align: right;
                white-space: nowrap;
                padding-top: 1px;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )


# =========================================================
# GENERAL HELPERS
# =========================================================
def parse_payload(payload):
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return {}
    try:
        return dict(payload)
    except Exception:
        return {}


def normalize_text_for_compare(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def is_blank(x) -> bool:
    if pd.isna(x):
        return True
    return str(x).strip() == ""


def try_parse_number(x):
    if pd.isna(x):
        return None

    s = str(x).strip()
    if s == "":
        return None

    s = s.replace("%", "").replace(" ", "")

    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return None


def should_hide_parameter(param: str) -> bool:
    p = str(param).strip().lower()
    if p == "source_row_id":
        return True
    if "source_name" in p:
        return True
    return False


def compare_values(left_val, right_val, tolerance_pct: float):
    if is_blank(left_val) and is_blank(right_val):
        return "empty_equal", "", ""

    left_num = try_parse_number(left_val)
    right_num = try_parse_number(right_val)

    if left_num is not None and right_num is not None:
        diff_abs = abs(left_num - right_num)

        if diff_abs == 0:
            return "exact_equal", "0%", "numeric"

        base = max(abs(left_num), abs(right_num))
        pct_diff = 0.0 if base == 0 else (diff_abs / base) * 100.0
        diff_text = f"{pct_diff:.2f}%"

        if pct_diff <= tolerance_pct:
            return "within_tolerance", diff_text, "numeric"

        return "different", diff_text, "numeric"

    left_txt = normalize_text_for_compare(left_val)
    right_txt = normalize_text_for_compare(right_val)

    if left_txt == right_txt:
        raw_left = "" if pd.isna(left_val) else str(left_val)
        raw_right = "" if pd.isna(right_val) else str(right_val)
        if raw_left == raw_right:
            return "exact_equal", "", "text"
        return "normalized_equal", "", "text"

    return "different", "", "text"


def build_combined_display_df(
    left_dict: dict,
    right_dict: dict,
    tolerance_pct: float,
    left_title: str,
    right_title: str,
) -> pd.DataFrame:
    all_params = list(dict.fromkeys(list(left_dict.keys()) + list(right_dict.keys())))
    all_params = [p for p in all_params if not should_hide_parameter(p)]

    rows = []
    for param in all_params:
        left_val = left_dict.get(param, "")
        right_val = right_dict.get(param, "")
        status, diff_text, compare_type = compare_values(left_val, right_val, tolerance_pct)

        if status == "empty_equal":
            continue

        rows.append(
            {
                "Parameter": str(param),
                left_title: "" if pd.isna(left_val) else str(left_val),
                right_title: "" if pd.isna(right_val) else str(right_val),
                "_status": status,
                "_diff": diff_text,
                "_compare_type": compare_type,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    status_rank = {
        "exact_equal": 0,
        "within_tolerance": 1,
        "normalized_equal": 1,
        "different": 2,
    }
    df["_keylvl3_priority"] = df["Parameter"].str.lower().apply(lambda x: 0 if x == "key_lvl3" else 1)
    df["_sort_rank"] = df["_status"].map(status_rank).fillna(99)
    df = df.sort_values(["_keylvl3_priority", "_sort_rank", "Parameter"], kind="stable").reset_index(drop=True)
    return df


def status_label(status: str, diff_text: str, compare_type: str) -> str:
    if status == "different":
        if compare_type == "numeric" and diff_text:
            return f"DIFF ({diff_text})"
        return "DIFF"
    if status == "within_tolerance":
        return f"TOL ({diff_text})" if diff_text else "TOL"
    if status == "normalized_equal":
        return "TEXT_EQ"
    if status == "exact_equal":
        return "EQ"
    return status


def render_comparison_table_html(df: pd.DataFrame, left_title: str, right_title: str, mobile_mode: bool) -> str:
    def bg(status: str) -> str:
        if status == "different":
            return "#f8d7da"
        if status in ("within_tolerance", "normalized_equal"):
            return "#fff3cd"
        if status == "exact_equal":
            return "#d1e7dd"
        return "#ffffff"

    table_height = "auto" if mobile_mode else "calc(100vh - 355px)"
    min_height = "unset" if mobile_mode else "280px"
    max_height = "none" if mobile_mode else "calc(100vh - 355px)"

    html_rows = []
    for _, row in df.iterrows():
        status = row["_status"]
        color = bg(status)
        html_rows.append(
            "<tr>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;'><b>{html.escape(str(row['Parameter']))}</b></td>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;white-space:pre-wrap;word-break:break-word;'>{html.escape(str(row[left_title]))}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;white-space:pre-wrap;word-break:break-word;'>{html.escape(str(row[right_title]))}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;'><b>{html.escape(status_label(status, row['_diff'], row['_compare_type']))}</b></td>"
            "</tr>"
        )

    return f"""
    <div style="
        height:{table_height};
        min-height:{min_height};
        max-height:{max_height};
        overflow-y:auto;
        overflow-x:auto;
        border:1px solid #ddd;
        border-radius:8px;
        box-sizing:border-box;
        background:white;
    ">
      <table style="border-collapse:collapse; width:100%; font-size:14px; table-layout:fixed;">
        <thead style="position:sticky; top:0; z-index:2;">
          <tr>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:20%;">Parameter</th>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:34%;">{html.escape(left_title)}</th>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:34%;">{html.escape(right_title)}</th>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:12%;">Status</th>
          </tr>
        </thead>
        <tbody>
          {''.join(html_rows)}
        </tbody>
      </table>
    </div>
    """


def render_mobile_compare_cards(df: pd.DataFrame, left_title: str, right_title: str):
    color_map = {
        "different": "#f8d7da",
        "within_tolerance": "#fff3cd",
        "normalized_equal": "#fff3cd",
        "exact_equal": "#d1e7dd",
    }

    for _, row in df.iterrows():
        color = color_map.get(row["_status"], "#ffffff")
        st.markdown(
            f"""
            <div style="
                background:{color};
                border:1px solid #ddd;
                border-radius:10px;
                padding:10px;
                margin-bottom:10px;
            ">
                <div><b>{html.escape(str(row['Parameter']))}</b></div>
                <div style="margin-top:6px;"><b>{html.escape(left_title)}:</b><br>{html.escape(str(row[left_title]))}</div>
                <div style="margin-top:6px;"><b>{html.escape(right_title)}:</b><br>{html.escape(str(row[right_title]))}</div>
                <div style="margin-top:6px;"><b>Status:</b> {html.escape(status_label(row['_status'], row['_diff'], row['_compare_type']))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# =========================================================
# DB HELPERS
# =========================================================
def ensure_lock_columns():
    query = text(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'review'
          and table_name = 'review_cases'
        """
    )
    with engine.connect() as conn:
        cols = {row[0] for row in conn.execute(query).fetchall()}
    return REQUIRED_LOCK_COLUMNS - cols


def maybe_cleanup_stale_locks(run_id: str):
    cleanup_map = st.session_state.setdefault("cleanup_ts_by_run", {})
    now_ts = datetime.utcnow().timestamp()
    last_ts = cleanup_map.get(run_id, 0.0)
    if now_ts - last_ts < 60:
        return

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                update review.review_cases
                set status = 'open',
                    locked_by = null,
                    locked_at = null,
                    updated_at = now()
                where run_id = :run_id
                  and status = 'in_review'
                  and locked_at is not null
                  and locked_at < now() - make_interval(mins => :timeout_minutes)
                """
            ),
            {"run_id": run_id, "timeout_minutes": CLAIM_TIMEOUT_MINUTES},
        )
    cleanup_map[run_id] = now_ts
    st.session_state["cleanup_ts_by_run"] = cleanup_map


def fetch_runs() -> pd.DataFrame:
    query = text(
        """
        select
            r.run_id,
            r.left_source,
            r.right_source,
            count(*) filter (where c.status = 'open') as open_case_count,
            count(*) filter (where c.status = 'in_review') as locked_case_count,
            max(c.updated_at) as last_case_update
        from review.review_runs r
        join review.review_cases c
          on r.run_id = c.run_id
        where c.status in ('open', 'in_review')
        group by r.run_id, r.left_source, r.right_source
        order by last_case_update desc nulls last, r.run_id desc
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def fetch_run_pair_keys(run_id: str):
    query = text(
        """
        select pair_key
        from review.review_cases
        where run_id = :run_id
        order by score_total asc nulls last, pair_key asc
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"run_id": run_id}).fetchall()
    return [str(row[0]) for row in rows]


def fetch_run_stats(run_id: str, reviewer: str) -> dict:
    query = text(
        """
        select
            count(*) filter (where status = 'open') as open_count,
            count(*) filter (where status = 'in_review') as locked_count,
            count(*) filter (where status = 'in_review' and locked_by = :reviewer) as my_locked_count,
            count(*) filter (where status = 'reviewed') as reviewed_count,
            count(*) as total_count
        from review.review_cases
        where run_id = :run_id
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"run_id": run_id, "reviewer": reviewer}).mappings().one()
    return dict(row)


def get_pair_keys_for_run(run_id: str):
    pair_keys_cache = st.session_state.setdefault("pair_keys_by_run", {})
    if run_id not in pair_keys_cache:
        pair_keys_cache[run_id] = fetch_run_pair_keys(run_id)
        st.session_state["pair_keys_by_run"] = pair_keys_cache
    return pair_keys_cache[run_id]


def row_mapping_to_dict(row):
    return dict(row) if row is not None else None


def claim_case_batch(run_id: str, reviewer: str, batch_size: int):
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                with next_cases as (
                    select pair_key
                    from review.review_cases
                    where run_id = :run_id
                      and status = 'open'
                    order by score_total asc nulls last, pair_key asc
                    for update skip locked
                    limit :batch_size
                )
                update review.review_cases c
                set status = 'in_review',
                    locked_by = :reviewer,
                    locked_at = now(),
                    updated_at = now()
                from next_cases
                where c.run_id = :run_id
                  and c.pair_key = next_cases.pair_key
                returning
                    c.pair_key,
                    c.run_id,
                    c.pair_id,
                    c.left_source,
                    c.right_source,
                    c.left_id,
                    c.right_id,
                    c.score_total,
                    c.left_payload,
                    c.right_payload,
                    c.status,
                    c.locked_by,
                    c.locked_at
                """
            ),
            {"run_id": run_id, "reviewer": reviewer, "batch_size": int(batch_size)},
        ).mappings().all()
    return [row_mapping_to_dict(row) for row in rows]


def release_case_batch(run_id: str, pair_keys: list[str], reviewer: str):
    if not pair_keys:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                update review.review_cases
                set status = 'open',
                    locked_by = null,
                    locked_at = null,
                    updated_at = now()
                where run_id = :run_id
                  and pair_key = any(:pair_keys)
                  and status = 'in_review'
                  and locked_by = :reviewer
                """
            ),
            {"run_id": run_id, "pair_keys": pair_keys, "reviewer": reviewer},
        )


def save_case_decision(pair_row: dict, decision: str, comment: str, reviewer: str) -> bool:
    with engine.begin() as conn:
        updated = conn.execute(
            text(
                """
                update review.review_cases
                set status = 'reviewed',
                    locked_by = null,
                    locked_at = null,
                    updated_at = now()
                where run_id = :run_id
                  and pair_key = :pair_key
                  and status = 'in_review'
                  and locked_by = :reviewer
                returning pair_id, left_id, right_id
                """
            ),
            {
                "run_id": pair_row["run_id"],
                "pair_key": pair_row["pair_key"],
                "reviewer": reviewer,
            },
        ).mappings().first()

        if updated is None:
            return False

        conn.execute(
            text(
                """
                insert into review.review_labels (
                    run_id,
                    pair_key,
                    pair_id,
                    left_id,
                    right_id,
                    decision,
                    comment,
                    reviewer,
                    timestamp
                )
                values (
                    :run_id,
                    :pair_key,
                    :pair_id,
                    :left_id,
                    :right_id,
                    :decision,
                    :comment,
                    :reviewer,
                    :timestamp
                )
                """
            ),
            {
                "run_id": pair_row["run_id"],
                "pair_key": pair_row["pair_key"],
                "pair_id": updated["pair_id"],
                "left_id": updated["left_id"],
                "right_id": updated["right_id"],
                "decision": decision,
                "comment": comment,
                "reviewer": reviewer,
                "timestamp": datetime.utcnow(),
            },
        )

        remaining = conn.execute(
            text(
                """
                select count(*)
                from review.review_cases
                where run_id = :run_id
                  and status in ('open', 'in_review')
                """
            ),
            {"run_id": pair_row["run_id"]},
        ).scalar_one()

        if int(remaining) == 0:
            conn.execute(
                text(
                    """
                    update review.review_runs
                    set status = 'reviewed',
                        updated_at = now()
                    where run_id = :run_id
                    """
                ),
                {"run_id": pair_row["run_id"]},
            )

    return True


# =========================================================
# SESSION / DRAFT HELPERS
# =========================================================
def run_label(row) -> str:
    left_source = str(row.get("left_source", ""))
    right_source = str(row.get("right_source", ""))
    open_case_count = int(row.get("open_case_count", 0) or 0)
    locked_case_count = int(row.get("locked_case_count", 0) or 0)
    return f"{left_source} vs {right_source} | offen: {open_case_count} | reserviert: {locked_case_count}"


def get_batch_state_key(run_id: str, reviewer: str) -> str:
    return f"batch_state::{run_id}::{reviewer}"


def reset_batch_state(run_id: str, reviewer: str):
    st.session_state.pop(get_batch_state_key(run_id, reviewer), None)


def get_batch_state(run_id: str, reviewer: str) -> dict:
    key = get_batch_state_key(run_id, reviewer)
    if key not in st.session_state:
        st.session_state[key] = {
            "queue": [],
            "current": None,
            "history": [],
            "batch_size": DEFAULT_BATCH_SIZE,
            "drafts": {},
            "rows_by_pair_key": {},
            "claimed_pair_keys": [],
        }
    return st.session_state[key]


def get_draft(batch_state: dict, pair_key: str) -> dict:
    return batch_state.setdefault("drafts", {}).get(str(pair_key), {})


def save_draft(batch_state: dict, pair_key: str, decision: str, comment: str):
    batch_state.setdefault("drafts", {})[str(pair_key)] = {
        "decision": decision,
        "comment": comment,
    }


def clear_draft(batch_state: dict, pair_key: str):
    batch_state.setdefault("drafts", {}).pop(str(pair_key), None)


def prepare_inputs_for_pair(batch_state: dict, pair_key: str):
    decision_key = f"decision_{pair_key}"
    comment_key = f"comment_{pair_key}"

    draft = get_draft(batch_state, pair_key)

    if decision_key not in st.session_state:
        st.session_state[decision_key] = draft.get("decision", DECISION_OPTIONS[0])

    if comment_key not in st.session_state:
        st.session_state[comment_key] = draft.get("comment", "")

    return decision_key, comment_key


def sync_draft_from_widgets(batch_state: dict, pair_key: str):
    decision_key = f"decision_{pair_key}"
    comment_key = f"comment_{pair_key}"
    save_draft(
        batch_state=batch_state,
        pair_key=pair_key,
        decision=st.session_state.get(decision_key, DECISION_OPTIONS[0]),
        comment=st.session_state.get(comment_key, ""),
    )


def push_history(batch_state: dict, pair_row: dict):
    if pair_row is None:
        return
    history = batch_state.get("history", [])
    history.append(pair_row)
    batch_state["history"] = history[-MAX_BACK_HISTORY:]


def pop_history(batch_state: dict):
    history = batch_state.get("history", [])
    if not history:
        return None
    prev = history.pop()
    batch_state["history"] = history
    return prev


def current_pair_keys_in_batch(batch_state: dict) -> list[str]:
    return list(batch_state.get("claimed_pair_keys", []))


def hydrate_batch_state_from_claimed_rows(batch_state: dict, claimed_rows: list[dict]):
    rows_by_pair_key = {}
    claimed_pair_keys = []

    for row in claimed_rows:
        pair_key = str(row["pair_key"])
        rows_by_pair_key[pair_key] = row
        claimed_pair_keys.append(pair_key)

    batch_state["rows_by_pair_key"] = rows_by_pair_key
    batch_state["claimed_pair_keys"] = claimed_pair_keys
    batch_state["queue"] = claimed_rows.copy()
    batch_state["current"] = batch_state["queue"].pop(0) if batch_state["queue"] else None
    batch_state["history"] = []
    batch_state["drafts"] = {}


def refill_batch_if_needed(run_id: str, reviewer: str, batch_state: dict):
    current_count = len(batch_state.get("queue", []))
    if batch_state.get("current") is not None:
        current_count += 1
    if current_count > 0:
        return

    claimed = claim_case_batch(run_id, reviewer, batch_state.get("batch_size", DEFAULT_BATCH_SIZE))
    hydrate_batch_state_from_claimed_rows(batch_state, claimed)


def move_to_next_local(run_id: str, reviewer: str, batch_state: dict, keep_current_in_history: bool = True):
    current = batch_state.get("current")
    if keep_current_in_history and current is not None:
        push_history(batch_state, current)

    if batch_state.get("queue"):
        batch_state["current"] = batch_state["queue"].pop(0)
    else:
        batch_state["current"] = None


def move_back_local(batch_state: dict):
    previous = pop_history(batch_state)
    if previous is None:
        return False
    current = batch_state.get("current")
    if current is not None:
        batch_state["queue"].insert(0, current)
    batch_state["current"] = previous
    return True


def release_all_batch_locks(run_id: str, reviewer: str, batch_state: dict):
    keys = current_pair_keys_in_batch(batch_state)
    if keys:
        release_case_batch(run_id, keys, reviewer)
    batch_state["queue"] = []
    batch_state["current"] = None
    batch_state["history"] = []
    batch_state["drafts"] = {}
    batch_state["rows_by_pair_key"] = {}
    batch_state["claimed_pair_keys"] = []


def initialize_batch_for_run(run_id: str, reviewer: str, batch_size: int):
    batch_state = get_batch_state(run_id, reviewer)
    old_size = int(batch_state.get("batch_size", DEFAULT_BATCH_SIZE))
    if old_size != int(batch_size) and current_pair_keys_in_batch(batch_state):
        release_all_batch_locks(run_id, reviewer, batch_state)
    batch_state["batch_size"] = int(batch_size)
    refill_batch_if_needed(run_id, reviewer, batch_state)
    return batch_state


def save_all_drafts_in_batch(run_id: str, reviewer: str, batch_state: dict):
    drafts = dict(batch_state.get("drafts", {}))
    if not drafts:
        return 0, 0

    saved_count = 0
    failed_count = 0
    rows_by_pair_key = dict(batch_state.get("rows_by_pair_key", {}))

    # Safety net: Falls Drafts fehlen (z. B. letzter Eintrag), aus claimed rows die Defaults nehmen.
    for pair_key in batch_state.get("claimed_pair_keys", []):
        if pair_key not in drafts:
            drafts[pair_key] = {
                "decision": DECISION_OPTIONS[0],
                "comment": "",
            }

    for pair_key, draft in drafts.items():
        pair_row = rows_by_pair_key.get(str(pair_key))
        if pair_row is None:
            failed_count += 1
            continue

        ok = save_case_decision(
            pair_row=pair_row,
            decision=draft.get("decision", DECISION_OPTIONS[0]),
            comment=draft.get("comment", ""),
            reviewer=reviewer,
        )
        if ok:
            saved_count += 1
        else:
            failed_count += 1

    return saved_count, failed_count


def render_sidebar_metric(title: str, left_value, right_value=None, progress_ratio=None):
    st.sidebar.markdown(f"<div class='sidebar-stat-title'>{html.escape(title)}</div>", unsafe_allow_html=True)
    left_col, right_col = st.sidebar.columns([4.5, 1.7])
    with left_col:
        if progress_ratio is None:
            progress_ratio = 0.0
        progress_ratio = max(0.0, min(1.0, float(progress_ratio)))
        st.progress(progress_ratio)
    with right_col:
        if right_value is None:
            text_value = f"{left_value}"
        else:
            text_value = f"{left_value} / {right_value}"
        st.markdown(f"<div class='sidebar-stat-value'>{html.escape(text_value)}</div>", unsafe_allow_html=True)


# =========================================================
# APP START
# =========================================================
mobile_mode = st.sidebar.toggle("Mobile-Modus", value=False)
apply_css(mobile_mode)
st.markdown('<div class="app-main-title">Blocking Review</div>', unsafe_allow_html=True)

missing_columns = ensure_lock_columns()
if missing_columns:
    st.error("In review.review_cases fehlen Lock-Spalten fuer Multi-User-Betrieb.")
    st.code(
        """
ALTER TABLE review.review_cases
ADD COLUMN IF NOT EXISTS locked_by text,
ADD COLUMN IF NOT EXISTS locked_at timestamptz;
        """.strip(),
        language="sql",
    )
    st.stop()

runs_df = fetch_runs()
if runs_df.empty:
    st.success("Keine offenen oder reservierten Runs vorhanden.")
    st.stop()

run_options = runs_df["run_id"].tolist()
run_label_map = {row["run_id"]: run_label(row) for _, row in runs_df.iterrows()}

reviewer_before = st.session_state.get("reviewer_name", "")
reviewer = st.sidebar.text_input(
    "Reviewer",
    value=reviewer_before,
    key="reviewer_name",
    placeholder="Pflichtfeld",
)

batch_size = st.sidebar.selectbox(
    "Batch-Groesse",
    options=[5, 10, 20, 50],
    index=[5, 10, 20, 50].index(DEFAULT_BATCH_SIZE),
    help="Groesserer Batch = schnelleres lokales Weiter/Zurueck, aber mehr initialer DB-Traffic.",
)

if not reviewer or not reviewer.strip():
    st.markdown('<div class="reviewer-required-box">', unsafe_allow_html=True)
    st.warning("Bitte zuerst einen Reviewer-Namen eingeben.")
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

reviewer = reviewer.strip()

previous_selected_run = st.session_state.get("selected_run_id")

selected_run_id = st.sidebar.selectbox(
    "Review-Run auswählen",
    options=["-- bitte wählen --"] + run_options,
    index=0 if "selected_run_id" not in st.session_state else (
        run_options.index(st.session_state["selected_run_id"]) + 1
        if st.session_state["selected_run_id"] in run_options else 0
    ),
    format_func=lambda x: run_label_map.get(x, x) if x != "-- bitte wählen --" else x,
)

if selected_run_id == "-- bitte wählen --":
    st.warning("Bitte Review-Run auswählen")
    st.stop()

st.session_state["selected_run_id"] = selected_run_id

# =========================================================
# INITIAL USER SELECTION GATE
# =========================================================

if selected_run_id is None:
    st.warning("Bitte Review-Run auswählen")
    st.stop()

if not batch_size:
    st.warning("Bitte Batch-Größe wählen")
    st.stop()

if previous_selected_run != selected_run_id and previous_selected_run in run_options:
    old_batch = get_batch_state(previous_selected_run, reviewer)
    release_all_batch_locks(previous_selected_run, reviewer, old_batch)
    reset_batch_state(previous_selected_run, reviewer)

if reviewer_before and reviewer_before != reviewer and previous_selected_run == selected_run_id:
    old_batch = get_batch_state(selected_run_id, reviewer_before)
    release_all_batch_locks(selected_run_id, reviewer_before, old_batch)
    reset_batch_state(selected_run_id, reviewer_before)

maybe_cleanup_stale_locks(selected_run_id)

pair_keys = get_pair_keys_for_run(selected_run_id)
session_total = len(pair_keys)
run_stats = fetch_run_stats(selected_run_id, reviewer)
batch_state = initialize_batch_for_run(selected_run_id, reviewer, int(batch_size))
pair_row = batch_state.get("current")

toast_key = f"batch_loaded_toast::{selected_run_id}::{reviewer}"

def get_batch_signature(batch_state):
    keys = []
    current = batch_state.get("current")
    if current:
        keys.append(current["pair_key"])
    for row in batch_state.get("queue", []):
        keys.append(row["pair_key"])
    return tuple(keys)

current_batch_signature = get_batch_signature(batch_state)
if st.session_state.get(toast_key) != current_batch_signature and current_batch_signature:
    try:
        st.toast(f"Batch geladen: {len(current_batch_signature)} Fälle")
    except Exception:
        pass
    st.session_state[toast_key] = current_batch_signature

if pair_row is None:
    st.success("Dieser Run hat aktuell keine frei verfuegbaren Faelle mehr.")
    st.caption("Entweder ist alles bearbeitet oder die restlichen Faelle sind gerade von anderen Reviewern reserviert.")
    st.stop()

current_pair_key = str(pair_row["pair_key"])
decision_key, comment_key = prepare_inputs_for_pair(batch_state, current_pair_key)
left_payload = parse_payload(pair_row["left_payload"])
right_payload = parse_payload(pair_row["right_payload"])
left_title_dynamic = str(pair_row.get("left_source", "Left"))
right_title_dynamic = str(pair_row.get("right_source", "Right"))

session_pair_number = pair_keys.index(current_pair_key) + 1 if current_pair_key in pair_keys else 1
progress_value = session_pair_number / session_total if session_total > 0 else 1.0

# =========================================================
# SIDEBAR STATUS
# =========================================================
reviewed_total = int(run_stats.get("reviewed_count", 0) or 0)
open_total = int(run_stats.get("open_count", 0) or 0)
draft_total = len(batch_state.get("drafts", {}))
remaining = 1 + len(batch_state.get("queue", []))
batch_size_total = batch_state.get("batch_size", DEFAULT_BATCH_SIZE)
history_total = len(batch_state.get("history", []))

render_sidebar_metric(
    "Bereits bearbeitet insgesamt / Noch offen insgesamt",
    reviewed_total,
    open_total,
    progress_ratio=(reviewed_total / (reviewed_total + open_total)) if (reviewed_total + open_total) > 0 else 0.0,
)
render_sidebar_metric(
    "Lokaler Batch",
    remaining,
    batch_size_total,
    progress_ratio=remaining / batch_size_total if batch_size_total > 0 else 0
)
render_sidebar_metric(
    "Zurueck History",
    history_total,
    MAX_BACK_HISTORY,
    progress_ratio=(history_total / MAX_BACK_HISTORY) if MAX_BACK_HISTORY > 0 else 0.0,
)

st.sidebar.markdown("---")
st.sidebar.write(f"Run ID: {selected_run_id}")

st.sidebar.markdown(f"Lokale Drafts im Batch: **{draft_total}**")
st.sidebar.markdown(f"Komplette Batch-Faelle reserviert: **{len(batch_state.get('claimed_pair_keys', []))}**")

tolerance_pct = st.sidebar.number_input(
    "Toleranz in Prozent",
    min_value=0.0,
    value=5.0,
    step=0.5,
    help="Wenn zwei numerische Werte sich hoechstens um diesen Prozentwert unterscheiden, werden sie gelb markiert.",
)

st.sidebar.markdown("**Farblegende**")
st.sidebar.markdown(
    """
    <div style="display:flex; gap:8px; align-items:center; margin-bottom:6px;">
      <div style="width:18px;height:18px;background:#d1e7dd;border:1px solid #ccc;"></div><div>Gruen = exakt gleich</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; margin-bottom:6px;">
      <div style="width:18px;height:18px;background:#fff3cd;border:1px solid #ccc;"></div><div>Gelb = innerhalb Prozent-Toleranz / Text fast gleich</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; margin-bottom:6px;">
      <div style="width:18px;height:18px;background:#f8d7da;border:1px solid #ccc;"></div><div>Rot = unterschiedlich</div>
    </div>
    """,
    unsafe_allow_html=True,
)

if mobile_mode:
    st.markdown(f"**Session-Paar {session_pair_number} / {session_total}**")
    st.progress(progress_value)

# =========================================================
# TOP INFO + DECISION
# =========================================================
score_val = pair_row["score_total"]
conf_text = "-"
if pd.notna(score_val):
    try:
        conf_text = f"{float(score_val):.4f}"
    except Exception:
        conf_text = str(score_val)

if mobile_mode:
    st.markdown(
        f"""
        <div class="top-score-card">
            <div class="top-score-label">Confidence Score</div>
            <div class="top-score-value">{html.escape(conf_text)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="review-section-title">Entscheidung</div>', unsafe_allow_html=True)
    with st.form(key=f"decision_form_{current_pair_key}"):
        decision = st.radio(
            "Ist diese Kombination im Blocking sinnvoll?",
            options=DECISION_OPTIONS,
            horizontal=False,
            key=decision_key,
            format_func=lambda x: {
                "BLOCK_OK": "Blocking passt",
                "BLOCK_NOK": "Blocking passt nicht",
                "UNSURE": "Unklar",
            }.get(x, x),
        )
        comment = st.text_area("Kommentar", height=120, key=comment_key)
        back = st.form_submit_button(
            "Zurueck",
            use_container_width=True,
            disabled=len(batch_state.get("history", [])) == 0,
        )
        next_local = st.form_submit_button("Weiter", use_container_width=True)
        save_batch_btn = st.form_submit_button("Batch speichern & neuen Batch holen", use_container_width=True)
else:
    header_left, header_right = st.columns([1.0, 2.2])

    with header_left:
        st.markdown(
            f"""
            <div class="top-score-card">
                <div class="top-score-label">Confidence Score</div>
                <div class="top-score-value">{html.escape(conf_text)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with header_right:
        st.markdown('<div class="review-section-title">Entscheidung</div>', unsafe_allow_html=True)
        with st.form(key=f"decision_form_{current_pair_key}"):
            decision_left, decision_right = st.columns([1.25, 1.0])
            with decision_left:
                decision = st.radio(
                    "Ist diese Kombination im Blocking sinnvoll?",
                    options=DECISION_OPTIONS,
                    horizontal=True,
                    key=decision_key,
                    format_func=lambda x: {
                        "BLOCK_OK": "Blocking passt",
                        "BLOCK_NOK": "Blocking passt nicht",
                        "UNSURE": "Unklar",
                    }.get(x, x),
                )
            with decision_right:
                comment = st.text_area("Kommentar", height=95, key=comment_key)
            btn1, btn2, btn3 = st.columns([1, 1, 2.2])
            with btn1:
                back = st.form_submit_button(
                    "Zurueck",
                    use_container_width=True,
                    disabled=len(batch_state.get("history", [])) == 0,
                )
            with btn2:
                next_local = st.form_submit_button("Weiter", use_container_width=True)
            with btn3:
                save_batch_btn = st.form_submit_button(
                    "Batch speichern & neuen Batch holen",
                    use_container_width=True,
                )

# Draft nach Form-Submit sichern
save_draft(batch_state, current_pair_key, decision, comment)

# =========================================================
# ACTIONS
# =========================================================
if back:
    moved = move_back_local(batch_state)
    if moved:
        prev_pair = batch_state.get("current")
        if prev_pair is not None:
            prev_pair_key = str(prev_pair["pair_key"])
            prev_decision_key = f"decision_{prev_pair_key}"
            prev_comment_key = f"comment_{prev_pair_key}"
            draft = get_draft(batch_state, prev_pair_key)
            st.session_state[prev_decision_key] = draft.get("decision", DECISION_OPTIONS[0])
            st.session_state[prev_comment_key] = draft.get("comment", "")
        st.rerun()

if next_local:
    # Aktuellen Eintrag vor Navigation sicher als Draft speichern
    save_draft(
        batch_state=batch_state,
        pair_key=current_pair_key,
        decision=st.session_state.get(decision_key, DECISION_OPTIONS[0]),
        comment=st.session_state.get(comment_key, ""),
    )

    # Prüfen: Bin ich gerade auf dem letzten Eintrag des aktuellen Batches?
    is_last_item_in_batch = batch_state.get("current") is not None and len(batch_state.get("queue", [])) == 0

    if is_last_item_in_batch:
        # Letzten Fall noch in die History schieben
        move_to_next_local(selected_run_id, reviewer, batch_state, keep_current_in_history=True)

        # Jetzt kompletten Batch speichern
        saved_count, failed_count = save_all_drafts_in_batch(selected_run_id, reviewer, batch_state)

        # Alte Locks freigeben und neuen Batch holen
        release_all_batch_locks(selected_run_id, reviewer, batch_state)
        refill_batch_if_needed(selected_run_id, reviewer, batch_state)

        next_pair = batch_state.get("current")
        if next_pair is not None:
            next_pair_key = str(next_pair["pair_key"])
            next_decision_key = f"decision_{next_pair_key}"
            next_comment_key = f"comment_{next_pair_key}"

            st.session_state[next_decision_key] = DECISION_OPTIONS[0]
            st.session_state[next_comment_key] = ""

            try:
                if failed_count == 0:
                    st.toast(f"Batch automatisch gespeichert und neuer Batch geladen: {saved_count} Fälle")
                else:
                    st.toast(f"Neuer Batch geladen. Gespeichert: {saved_count}, Fehler: {failed_count}")
            except Exception:
                pass
        else:
            try:
                if failed_count == 0:
                    st.toast(f"Letzter Batch automatisch gespeichert: {saved_count} Fälle")
                else:
                    st.toast(f"Letzter Batch verarbeitet. Gespeichert: {saved_count}, Fehler: {failed_count}")
            except Exception:
                pass

        st.rerun()

    # Normaler lokaler Weiter-Klick innerhalb des Batches
    move_to_next_local(selected_run_id, reviewer, batch_state, keep_current_in_history=True)

    new_current = batch_state.get("current")
    if new_current is not None:
        new_pair_key = str(new_current["pair_key"])
        new_decision_key = f"decision_{new_pair_key}"
        new_comment_key = f"comment_{new_pair_key}"
        draft = get_draft(batch_state, new_pair_key)
        st.session_state[new_decision_key] = draft.get("decision", DECISION_OPTIONS[0])
        st.session_state[new_comment_key] = draft.get("comment", "")

    st.rerun()

if save_batch_btn:
    # aktuellen Eintrag vor Batch-Save sichern
    save_draft(
        batch_state=batch_state,
        pair_key=current_pair_key,
        decision=st.session_state.get(decision_key, DECISION_OPTIONS[0]),
        comment=st.session_state.get(comment_key, ""),
    )

    saved_count, failed_count = save_all_drafts_in_batch(selected_run_id, reviewer, batch_state)

    release_all_batch_locks(selected_run_id, reviewer, batch_state)
    refill_batch_if_needed(selected_run_id, reviewer, batch_state)

    next_pair = batch_state.get("current")
    if next_pair is not None:
        next_pair_key = str(next_pair["pair_key"])
        next_decision_key = f"decision_{next_pair_key}"
        next_comment_key = f"comment_{next_pair_key}"
        st.session_state[next_decision_key] = DECISION_OPTIONS[0]
        st.session_state[next_comment_key] = ""

        try:
            if failed_count == 0:
                st.toast(f"Batch gespeichert und neuer Batch geladen: {saved_count} Fälle")
            else:
                st.toast(f"Neuer Batch geladen. Gespeichert: {saved_count}, Fehler: {failed_count}")
        except Exception:
            pass
    else:
        if failed_count == 0:
            try:
                st.toast(f"{saved_count} Fälle gespeichert")
            except Exception:
                pass
        else:
            st.warning(f"{saved_count} gespeichert, {failed_count} konnten nicht gespeichert werden.")

    st.rerun()

# =========================================================
# COMPARISON TABLE
# =========================================================
comparison_df = build_combined_display_df(
    left_payload,
    right_payload,
    tolerance_pct,
    left_title_dynamic,
    right_title_dynamic,
)

if comparison_df.empty:
    st.info("Keine vergleichbaren Werte vorhanden.")
elif mobile_mode:
    render_mobile_compare_cards(comparison_df, left_title_dynamic, right_title_dynamic)
else:
    comparison_html = render_comparison_table_html(
        comparison_df,
        left_title_dynamic,
        right_title_dynamic,
        mobile_mode=False,
    )
    st.markdown(comparison_html, unsafe_allow_html=True)
