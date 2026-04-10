import json
import html
import re
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Blocking Review", layout="wide")

DECISION_OPTIONS = ["BLOCK_OK", "BLOCK_NOK", "UNSURE"]
CLAIM_TIMEOUT_MINUTES = 30
REQUIRED_LOCK_COLUMNS = {"locked_by", "locked_at"}
BATCH_SIZE = 50


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
            html, body, [data-testid="stAppViewContainer"], .main {
                height: auto !important;
                overflow: auto !important;
            }
            [data-testid="stHeader"] {
                height: 2.6rem !important;
                min-height: 2.6rem !important;
            }
            .block-container {
                padding-top: 0.45rem !important;
                padding-bottom: 1rem !important;
                max-width: 100% !important;
                height: auto !important;
                overflow: visible !important;
            }
            [data-testid="stSidebar"] { overflow-y: auto !important; }
            .app-main-title {
                display: block;
                font-size: 2.25rem;
                font-weight: 800;
                line-height: 1.18;
                margin-top: 0.1rem;
                margin-bottom: 0.9rem;
                padding-top: 0.1rem;
            }
            .review-section-title {
                font-size: 18px;
                font-weight: 700;
                margin-top: 6px;
                margin-bottom: 8px;
            }
            .top-score-card {
                border: 1px solid #dfe3e8;
                border-radius: 10px;
                padding: 8px 10px;
                background: #f8f9fa;
                margin-bottom: 10px;
                text-align: center;
            }
            .top-score-label { font-size: 12px; color: #6c757d; margin-bottom: 2px; }
            .top-score-value { font-size: 24px; font-weight: 800; line-height: 1.1; }
            .compact-note { font-size: 13px; color: #6c757d; }
            </style>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <style>
            html, body, [data-testid="stAppViewContainer"], .main {
                height: 100vh !important;
                overflow: hidden !important;
            }
            [data-testid="stHeader"] {
                height: 2.8rem !important;
                min-height: 2.8rem !important;
            }
            .block-container {
                padding-top: 0.55rem !important;
                padding-bottom: 0.4rem !important;
                max-width: 100% !important;
                height: 100vh !important;
                overflow: hidden !important;
            }
            [data-testid="stSidebar"] { overflow-y: auto !important; }
            .app-main-title {
                display: block;
                font-size: 2.65rem;
                font-weight: 800;
                line-height: 1.12;
                margin-top: 0.1rem;
                margin-bottom: 0.9rem;
                padding-top: 0.05rem;
            }
            .review-section-title {
                font-size: 18px;
                font-weight: 700;
                margin-top: 6px;
                margin-bottom: 8px;
            }
            .top-score-card {
                border: 1px solid #dfe3e8;
                border-radius: 10px;
                padding: 10px 14px;
                background: #f8f9fa;
                margin-bottom: 10px;
                text-align: center;
            }
            .top-score-label { font-size: 14px; color: #6c757d; margin-bottom: 2px; }
            .top-score-value { font-size: 34px; font-weight: 800; line-height: 1.1; }
            .compact-note { font-size: 13px; color: #6c757d; }
            </style>
            """,
            unsafe_allow_html=True,
        )


# =========================================================
# DB / MULTI USER HELPERS
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
    missing = REQUIRED_LOCK_COLUMNS - cols
    return missing


def load_open_runs_db() -> pd.DataFrame:
    query = text(
        """
        select
            r.run_id,
            r.left_source,
            r.right_source,
            r.status,
            r.created_at,
            r.updated_at,
            count(c.pair_key) filter (where c.status = 'open') as open_case_count,
            count(c.pair_key) filter (where c.status = 'in_review') as locked_case_count
        from review.review_runs r
        join review.review_cases c
            on r.run_id = c.run_id
        where c.status in ('open', 'in_review')
        group by
            r.run_id,
            r.left_source,
            r.right_source,
            r.status,
            r.created_at,
            r.updated_at
        order by
            r.updated_at desc nulls last,
            r.created_at desc nulls last,
            r.run_id desc
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def load_run_stats_db(run_id: str, reviewer: str) -> dict:
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


def load_reviewed_count_db(run_id: str) -> int:
    query = text(
        """
        select count(*)
        from review.review_cases
        where run_id = :run_id
          and status = 'reviewed'
        """
    )
    with engine.connect() as conn:
        return int(conn.execute(query, {"run_id": run_id}).scalar_one())


def clear_runtime_caches():
    st.session_state.pop("runs_df_cache", None)
    st.session_state.pop("runs_df_cache_ts", None)
    st.session_state.pop("run_stats_cache", None)
    st.session_state.pop("reviewed_count_cache", None)




def get_runs_df(force_refresh: bool = False) -> pd.DataFrame:
    now_ts = datetime.utcnow().timestamp()
    cached_df = st.session_state.get("runs_df_cache")
    cached_ts = st.session_state.get("runs_df_cache_ts", 0.0)
    if (not force_refresh) and cached_df is not None and (now_ts - cached_ts) < 10:
        return cached_df
    df = load_open_runs_db()
    st.session_state["runs_df_cache"] = df
    st.session_state["runs_df_cache_ts"] = now_ts
    return df


def get_run_stats(run_id: str, reviewer: str, force_refresh: bool = False) -> dict:
    now_ts = datetime.utcnow().timestamp()
    cache = st.session_state.get("run_stats_cache", {})
    cache_key = f"{run_id}|{reviewer}"
    if (not force_refresh) and cache_key in cache and (now_ts - cache[cache_key]["ts"]) < 5:
        return cache[cache_key]["value"]
    value = load_run_stats_db(run_id, reviewer)
    cache[cache_key] = {"ts": now_ts, "value": value}
    st.session_state["run_stats_cache"] = cache
    return value


def get_reviewed_count(run_id: str, force_refresh: bool = False) -> int:
    now_ts = datetime.utcnow().timestamp()
    cache = st.session_state.get("reviewed_count_cache", {})
    if (not force_refresh) and run_id in cache and (now_ts - cache[run_id]["ts"]) < 5:
        return cache[run_id]["value"]
    value = load_reviewed_count_db(run_id)
    cache[run_id] = {"ts": now_ts, "value": value}
    st.session_state["reviewed_count_cache"] = cache
    return value


def maybe_cleanup_stale_claims(run_id: str | None = None, force: bool = False):
    now_ts = datetime.utcnow().timestamp()
    last_ts = st.session_state.get("last_stale_cleanup_ts", 0.0)
    if force or (now_ts - last_ts) > 60:
        cleanup_stale_claims(run_id)
        st.session_state["last_stale_cleanup_ts"] = now_ts

def cleanup_stale_claims(run_id: str | None = None):
    where_run = "and run_id = :run_id" if run_id else ""
    params = {"minutes": CLAIM_TIMEOUT_MINUTES}
    if run_id:
        params["run_id"] = run_id
    query = text(
        f"""
        update review.review_cases
        set status = 'open',
            locked_by = null,
            locked_at = null,
            updated_at = now()
        where status = 'in_review'
          and locked_at < now() - (:minutes * interval '1 minute')
          {where_run}
        """
    )
    with engine.begin() as conn:
        conn.execute(query, params)


def release_claims_for_reviewer(run_id: str, reviewer: str, pair_keys: list[str] | None = None, only_unsaved: bool = False):
    if not reviewer:
        return
    where_keys = ""
    params = {"run_id": run_id, "reviewer": reviewer}
    if pair_keys:
        where_keys = "and pair_key = any(:pair_keys)"
        params["pair_keys"] = pair_keys
    if only_unsaved:
        where_keys += " and status = 'in_review'"
    query = text(
        f"""
        update review.review_cases
        set status = 'open',
            locked_by = null,
            locked_at = null,
            updated_at = now()
        where run_id = :run_id
          and locked_by = :reviewer
          and status = 'in_review'
          {where_keys}
        """
    )
    with engine.begin() as conn:
        conn.execute(query, params)


def claim_batch(run_id: str, reviewer: str, batch_size: int = BATCH_SIZE, exclude_pair_keys: list[str] | None = None) -> pd.DataFrame:
    exclude_pair_keys = [str(x) for x in (exclude_pair_keys or []) if x]
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
                  and locked_at < now() - (:minutes * interval '1 minute')
                """
            ),
            {"run_id": run_id, "minutes": CLAIM_TIMEOUT_MINUTES},
        )

        conn.execute(
            text(
                """
                update review.review_cases
                set locked_at = now(),
                    updated_at = now()
                where run_id = :run_id
                  and status = 'in_review'
                  and locked_by = :reviewer
                """
            ),
            {"run_id": run_id, "reviewer": reviewer},
        )

        candidate_query = text(
            """
            with candidate as (
                select pair_key
                from review.review_cases
                where run_id = :run_id
                  and status = 'open'
                  and (:exclude_len = 0 or pair_key <> all(:exclude_pair_keys))
                order by score_total asc nulls last, pair_key asc
                for update skip locked
                limit :batch_size
            )
            update review.review_cases c
            set status = 'in_review',
                locked_by = :reviewer,
                locked_at = now(),
                updated_at = now()
            from candidate
            where c.run_id = :run_id
              and c.pair_key = candidate.pair_key
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
        )
        rows = conn.execute(
            candidate_query,
            {
                "run_id": run_id,
                "reviewer": reviewer,
                "batch_size": batch_size,
                "exclude_pair_keys": exclude_pair_keys,
                "exclude_len": len(exclude_pair_keys),
            },
        ).mappings().fetchall()

        claimed_query = text(
            """
            select
                pair_key,
                run_id,
                pair_id,
                left_source,
                right_source,
                left_id,
                right_id,
                score_total,
                left_payload,
                right_payload,
                status,
                locked_by,
                locked_at
            from review.review_cases
            where run_id = :run_id
              and status = 'in_review'
              and locked_by = :reviewer
            order by score_total asc nulls last, pair_key asc
            limit :batch_size
            """
        )
        claimed_rows = conn.execute(
            claimed_query,
            {"run_id": run_id, "reviewer": reviewer, "batch_size": batch_size},
        ).mappings().fetchall()

    return pd.DataFrame([dict(r) for r in claimed_rows])


def save_decision(pair_row, decision: str, comment: str, reviewer: str):
    with engine.begin() as conn:
        pair_key = pair_row["pair_key"]
        run_id = pair_row["run_id"]

        current = conn.execute(
            text(
                """
                select status, locked_by
                from review.review_cases
                where run_id = :run_id
                  and pair_key = :pair_key
                for update
                """
            ),
            {"run_id": run_id, "pair_key": pair_key},
        ).mappings().first()

        if not current:
            return False, "Fall nicht mehr gefunden."
        if current["status"] == "reviewed":
            return False, "Dieser Fall wurde bereits von jemand anderem gespeichert."
        if current["status"] != "in_review" or current["locked_by"] != reviewer:
            return False, "Dieser Fall ist nicht mehr fuer dich reserviert."

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
                select
                    :run_id,
                    :pair_key,
                    :pair_id,
                    :left_id,
                    :right_id,
                    :decision,
                    :comment,
                    :reviewer,
                    :timestamp
                where not exists (
                    select 1
                    from review.review_labels
                    where run_id = :run_id
                      and pair_key = :pair_key
                )
                """
            ),
            {
                "run_id": pair_row["run_id"],
                "pair_key": pair_row["pair_key"],
                "pair_id": pair_row["pair_id"],
                "left_id": pair_row["left_id"],
                "right_id": pair_row["right_id"],
                "decision": decision,
                "comment": comment,
                "reviewer": reviewer,
                "timestamp": datetime.utcnow(),
            },
        )

        conn.execute(
            text(
                """
                update review.review_cases
                set status = 'reviewed',
                    locked_by = null,
                    locked_at = null,
                    updated_at = now()
                where pair_key = :pair_key
                  and run_id = :run_id
                """
            ),
            {"pair_key": pair_row["pair_key"], "run_id": pair_row["run_id"]},
        )

        remaining_open = conn.execute(
            text(
                """
                select count(*) as n
                from review.review_cases
                where run_id = :run_id
                  and status in ('open', 'in_review')
                """
            ),
            {"run_id": pair_row["run_id"]},
        ).scalar_one()

        if remaining_open == 0:
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

    return True, "Gespeichert"


# =========================================================
# GENERIC HELPERS
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
    return p == "source_row_id" or "source_name" in p


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


def build_combined_display_df(left_dict: dict, right_dict: dict, tolerance_pct: float, left_title: str, right_title: str) -> pd.DataFrame:
    all_params = list(dict.fromkeys(list(left_dict.keys()) + list(right_dict.keys())))
    all_params = [p for p in all_params if not should_hide_parameter(p)]

    priority_params = ["key_lvl3"]
    remaining_params = [p for p in all_params if p not in priority_params]
    ordered_params = priority_params + remaining_params

    rows = []
    for param in ordered_params:
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
    df["_priority"] = df["Parameter"].apply(lambda x: 0 if x == "key_lvl3" else 1)
    df["_sort_rank"] = df["_status"].map(status_rank).fillna(99)
    df = df.sort_values(["_priority", "_sort_rank", "Parameter"], kind="stable").reset_index(drop=True)
    return df


def status_label(status: str, diff_text: str, compare_type: str) -> str:
    if status == "different":
        return f"DIFF ({diff_text})" if compare_type == "numeric" and diff_text else "DIFF"
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

    table_height = "auto" if mobile_mode else "calc(100vh - 380px)"
    min_height = "unset" if mobile_mode else "280px"
    max_height = "none" if mobile_mode else "calc(100vh - 380px)"

    html_rows = []
    for _, row in df.iterrows():
        color = bg(row["_status"])
        html_rows.append(
            "<tr>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;'><b>{html.escape(str(row['Parameter']))}</b></td>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;white-space:pre-wrap;word-break:break-word;'>{html.escape(str(row[left_title]))}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;white-space:pre-wrap;word-break:break-word;'>{html.escape(str(row[right_title]))}</td>"
            f"<td style='padding:8px;border:1px solid #ddd;background:{color};vertical-align:top;'><b>{html.escape(status_label(row['_status'], row['_diff'], row['_compare_type']))}</b></td>"
            "</tr>"
        )

    return f"""
    <div style="height:{table_height}; min-height:{min_height}; max-height:{max_height}; overflow-y:auto; overflow-x:auto; border:1px solid #ddd; border-radius:8px; box-sizing:border-box; background:white;">
      <table style="border-collapse:collapse; width:100%; font-size:14px; table-layout:fixed;">
        <thead style="position:sticky; top:0; z-index:2;">
          <tr>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:20%;">Parameter</th>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:34%;">{html.escape(left_title)}</th>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:34%;">{html.escape(right_title)}</th>
            <th style="text-align:left; padding:10px; border:1px solid #ddd; background:#f1f3f5; width:12%;">Status</th>
          </tr>
        </thead>
        <tbody>{''.join(html_rows)}</tbody>
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
            <div style="background:{color}; border:1px solid #ddd; border-radius:10px; padding:10px; margin-bottom:10px;">
                <div><b>{html.escape(str(row['Parameter']))}</b></div>
                <div style="margin-top:6px;"><b>{html.escape(left_title)}:</b><br>{html.escape(str(row[left_title]))}</div>
                <div style="margin-top:6px;"><b>{html.escape(right_title)}:</b><br>{html.escape(str(row[right_title]))}</div>
                <div style="margin-top:6px;"><b>Status:</b> {html.escape(status_label(row['_status'], row['_diff'], row['_compare_type']))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def show_popup_message(message: str, duration_ms: int = 900):
    text_msg = html.escape(message)
    popup_html = f"""
    <div id="save-popup-overlay" style="position:fixed; inset:0; z-index:999998; background:rgba(128,128,128,0.45); display:flex; align-items:center; justify-content:center; opacity:1; transition:opacity 0.3s ease; backdrop-filter:blur(1px);">
        <div id="save-popup" style="background:white; color:#222; padding:16px 22px; border-radius:12px; box-shadow:0 12px 32px rgba(0,0,0,0.25); font-weight:600; font-size:15px; min-width:180px; text-align:center;">
            {text_msg}
        </div>
    </div>
    <script>
        setTimeout(function() {{
            var overlay = window.parent.document.getElementById("save-popup-overlay");
            if (overlay) {{
                overlay.style.opacity = "0";
                setTimeout(function() {{ if (overlay) overlay.remove(); }}, 350);
            }}
        }}, {duration_ms});
    </script>
    """
    st.components.v1.html(popup_html, height=0)


def run_label(row) -> str:
    return f"{row.get('left_source', '')} vs {row.get('right_source', '')} | offen: {row.get('open_case_count', 0)}"


def init_batch_state(selected_run_id: str, reviewer: str, force_reset: bool = False):
    batch_signature = f"{selected_run_id}|{reviewer}"
    if force_reset or st.session_state.get("batch_signature") != batch_signature:
        st.session_state["batch_signature"] = batch_signature
        st.session_state["selected_run_id"] = selected_run_id
        st.session_state["batch_df"] = pd.DataFrame()
        st.session_state["batch_keys"] = []
        st.session_state["batch_cursor"] = 0
        st.session_state["decision_map"] = {}
        st.session_state["comment_map"] = {}
        st.session_state["saved_keys"] = []


def ensure_batch_loaded(run_id: str, reviewer: str, force_reload: bool = False):
    batch_df = st.session_state.get("batch_df")
    batch_keys = st.session_state.get("batch_keys", [])
    need_load = (
        force_reload
        or batch_df is None
        or batch_df.empty
        or len(batch_keys) == 0
        or st.session_state.get("batch_cursor", 0) >= len(batch_keys)
    )
    if not need_load:
        return

    exclude_keys = st.session_state.get("saved_keys", [])
    claimed_df = claim_batch(run_id, reviewer, batch_size=BATCH_SIZE, exclude_pair_keys=exclude_keys)
    st.session_state["batch_df"] = claimed_df.copy()
    st.session_state["batch_keys"] = claimed_df["pair_key"].astype(str).tolist() if not claimed_df.empty else []
    st.session_state["batch_cursor"] = 0
    clear_runtime_caches()


def get_current_pair_row():
    batch_df = st.session_state.get("batch_df")
    batch_keys = st.session_state.get("batch_keys", [])
    cursor = st.session_state.get("batch_cursor", 0)
    if batch_df is None or batch_df.empty or not batch_keys:
        return None
    if cursor < 0 or cursor >= len(batch_keys):
        return None
    current_key = batch_keys[cursor]
    row_df = batch_df[batch_df["pair_key"].astype(str) == str(current_key)]
    if row_df.empty:
        return None
    return row_df.iloc[0]


def current_batch_position() -> int:
    return st.session_state.get("batch_cursor", 0) + 1


def on_run_change():
    old_run = st.session_state.get("selected_run_id")
    reviewer = st.session_state.get("reviewer_name", "user").strip()
    old_batch_keys = st.session_state.get("batch_keys", [])
    if old_run and reviewer and old_batch_keys:
        try:
            release_claims_for_reviewer(old_run, reviewer, pair_keys=old_batch_keys, only_unsaved=True)
        except Exception:
            pass
    st.session_state["selected_run_id"] = st.session_state["run_selectbox"]
    st.session_state["force_reset_run"] = True
    clear_runtime_caches()


# =========================================================
# APP START
# =========================================================
missing_lock_columns = ensure_lock_columns()
if missing_lock_columns:
    st.error("Multi-User-Locking braucht neue Spalten in review.review_cases.")
    st.code(
        """
ALTER TABLE review.review_cases
ADD COLUMN IF NOT EXISTS locked_by text,
ADD COLUMN IF NOT EXISTS locked_at timestamptz;

CREATE INDEX IF NOT EXISTS ix_review_cases_run_status_score
ON review.review_cases (run_id, status, score_total, pair_key);

CREATE UNIQUE INDEX IF NOT EXISTS ux_review_labels_run_pair
ON review.review_labels (run_id, pair_key);
        """.strip(),
        language="sql",
    )
    st.stop()

mobile_mode = st.sidebar.toggle("Mobile-Modus", value=False)
apply_css(mobile_mode)
st.markdown('<div class="app-main-title">Blocking Review</div>', unsafe_allow_html=True)

maybe_cleanup_stale_claims(force=True)
runs_df = get_runs_df()

if len(runs_df) == 0:
    st.success("Keine offenen Runs vorhanden.")
    st.stop()

run_options = runs_df["run_id"].tolist()
run_label_map = {row["run_id"]: run_label(row) for _, row in runs_df.iterrows()}

if "selected_run_id" not in st.session_state or st.session_state["selected_run_id"] not in run_options:
    st.session_state["selected_run_id"] = run_options[0]

selected_run_id = st.sidebar.selectbox(
    "Review-Run auswählen",
    options=run_options,
    index=run_options.index(st.session_state["selected_run_id"]),
    format_func=lambda x: run_label_map.get(x, x),
    key="run_selectbox",
    on_change=on_run_change,
)
selected_run_id = st.session_state.get("selected_run_id", selected_run_id)

reviewer = st.sidebar.text_input(
    "Reviewer",
    value=st.session_state.get("reviewer_name", "user"),
    key="reviewer_name",
)
reviewer = reviewer.strip()
if not reviewer:
    st.warning("Bitte Reviewer eintragen.")
    st.stop()

force_reset_run = st.session_state.pop("force_reset_run", False)
init_batch_state(selected_run_id, reviewer, force_reset=force_reset_run)
ensure_batch_loaded(selected_run_id, reviewer, force_reload=force_reset_run)

pair_row = get_current_pair_row()
stats = get_run_stats(selected_run_id, reviewer)

if pair_row is None:
    if (stats.get("open_count", 0) + stats.get("my_locked_count", 0)) == 0:
        st.success("Dieser Run hat keine offenen Faelle mehr fuer dich.")
    else:
        st.info("Aktuell ist kein freier Batch verfuegbar. Bitte Seite neu laden oder spaeter erneut versuchen.")
    clear_runtime_caches()
    st.stop()

current_pair_key = str(pair_row["pair_key"])
left_payload = parse_payload(pair_row["left_payload"])
right_payload = parse_payload(pair_row["right_payload"])
left_title_dynamic = str(pair_row.get("left_source", "Left"))
right_title_dynamic = str(pair_row.get("right_source", "Right"))

batch_keys = st.session_state.get("batch_keys", [])
session_pair_number = current_batch_position()
session_total = len(batch_keys)
session_reviewed_count = get_reviewed_count(selected_run_id)
progress_value = session_pair_number / session_total if session_total > 0 else 1.0
my_locked_count = stats.get("my_locked_count", 0)
locked_count = stats.get("locked_count", 0)
open_count = stats.get("open_count", 0)

current_decision = st.session_state.get("decision_map", {}).get(current_pair_key, DECISION_OPTIONS[0])
current_comment = st.session_state.get("comment_map", {}).get(current_pair_key, "")

# =========================================================
# SIDEBAR STATUS
# =========================================================
st.sidebar.write(f"Batch-Paar {session_pair_number} / {session_total}")
st.sidebar.progress(progress_value)
st.sidebar.write(f"Bereits bearbeitet: {session_reviewed_count}")
st.sidebar.write(f"Offen gesamt: {open_count}")
st.sidebar.write(f"Durch andere gesperrt: {max(locked_count - my_locked_count, 0)}")
st.sidebar.write(f"Mein Batch: {session_total}")
st.sidebar.write(f"Run ID: {selected_run_id}")

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
    st.markdown(f"**Batch-Paar {session_pair_number} / {session_total}**")
    st.progress(progress_value)
    st.caption(f"Bereits bearbeitet: {session_reviewed_count} | Offen: {open_count} | Mein Batch: {session_total}")

score_val = pair_row["score_total"]
conf_text = "-"
if pd.notna(score_val):
    try:
        conf_text = f"{float(score_val):.4f}"
    except Exception:
        conf_text = str(score_val)

lock_info = ""
if pair_row.get("locked_at") is not None:
    lock_info = f"<br>Reserviert fuer: {html.escape(str(reviewer))}"

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
    st.markdown(
        f"""
        <div class="compact-note">
            Batch-Paar {session_pair_number} / {session_total}<br>
            {html.escape(left_title_dynamic)} vs {html.escape(right_title_dynamic)}{lock_info}
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.form(key=f"decision_form_{current_pair_key}", clear_on_submit=False):
        st.markdown('<div class="review-section-title">Entscheidung</div>', unsafe_allow_html=True)
        decision = st.radio(
            "Ist diese Kombination im Blocking sinnvoll?",
            options=DECISION_OPTIONS,
            horizontal=False,
            index=DECISION_OPTIONS.index(current_decision),
            format_func=lambda x: {"BLOCK_OK": "Blocking passt", "BLOCK_NOK": "Blocking passt nicht", "UNSURE": "Unklar"}.get(x, x),
        )
        comment = st.text_area("Kommentar", value=current_comment, height=120)
        btn1, btn2, btn3 = st.columns(3)
        with btn1:
            back = st.form_submit_button("Zurueck", use_container_width=True)
        with btn2:
            next_only = st.form_submit_button("Weiter", use_container_width=True)
        with btn3:
            save_next = st.form_submit_button("Speichern und Weiter", use_container_width=True)
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
        st.markdown(
            f"""
            <div class="compact-note">
                Batch-Paar {session_pair_number} / {session_total}<br>
                {html.escape(left_title_dynamic)} vs {html.escape(right_title_dynamic)}{lock_info}
            </div>
            """,
            unsafe_allow_html=True,
        )
    with header_right:
        with st.form(key=f"decision_form_{current_pair_key}", clear_on_submit=False):
            st.markdown('<div class="review-section-title">Entscheidung</div>', unsafe_allow_html=True)
            decision_left, decision_right = st.columns([1.25, 1.0])
            with decision_left:
                decision = st.radio(
                    "Ist diese Kombination im Blocking sinnvoll?",
                    options=DECISION_OPTIONS,
                    horizontal=True,
                    index=DECISION_OPTIONS.index(current_decision),
                    format_func=lambda x: {"BLOCK_OK": "Blocking passt", "BLOCK_NOK": "Blocking passt nicht", "UNSURE": "Unklar"}.get(x, x),
                )
            with decision_right:
                comment = st.text_area("Kommentar", value=current_comment, height=95)
            btn1, btn2, btn3 = st.columns([1, 1, 1.4])
            with btn1:
                back = st.form_submit_button("Zurueck", use_container_width=True)
            with btn2:
                next_only = st.form_submit_button("Weiter", use_container_width=True)
            with btn3:
                save_next = st.form_submit_button("Speichern und Weiter", use_container_width=True)

st.session_state.setdefault("decision_map", {})[current_pair_key] = decision
st.session_state.setdefault("comment_map", {})[current_pair_key] = comment

# =========================================================
# NAVIGATION ACTIONS
# =========================================================
if back:
    if st.session_state.get("batch_cursor", 0) > 0:
        st.session_state["batch_cursor"] = st.session_state.get("batch_cursor", 0) - 1
    st.rerun()

if next_only:
    if st.session_state.get("batch_cursor", 0) < len(batch_keys) - 1:
        st.session_state["batch_cursor"] = st.session_state.get("batch_cursor", 0) + 1
    else:
        ensure_batch_loaded(selected_run_id, reviewer, force_reload=True)
    st.rerun()

if save_next:
    ok, msg = save_decision(pair_row, decision, comment, reviewer)
    if ok:
        saved_key = current_pair_key
        st.session_state.setdefault("saved_keys", []).append(saved_key)
        batch_df = st.session_state.get("batch_df", pd.DataFrame()).copy()
        if not batch_df.empty:
            batch_df = batch_df[batch_df["pair_key"].astype(str) != saved_key].reset_index(drop=True)
        st.session_state["batch_df"] = batch_df
        st.session_state["batch_keys"] = batch_df["pair_key"].astype(str).tolist() if not batch_df.empty else []
        st.session_state["decision_map"].pop(saved_key, None)
        st.session_state["comment_map"].pop(saved_key, None)
        current_cursor = st.session_state.get("batch_cursor", 0)
        new_len = len(st.session_state["batch_keys"])
        if new_len == 0:
            st.session_state["batch_cursor"] = 0
            ensure_batch_loaded(selected_run_id, reviewer, force_reload=True)
        else:
            st.session_state["batch_cursor"] = min(current_cursor, new_len - 1)
        clear_runtime_caches()
        try:
            st.toast(msg)
        except Exception:
            pass
        show_popup_message(msg, duration_ms=900)
    else:
        st.error(msg)
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
    st.info("Keine vergleichbaren Parameter vorhanden.")
elif mobile_mode:
    render_mobile_compare_cards(comparison_df, left_title_dynamic, right_title_dynamic)
else:
    comparison_html = render_comparison_table_html(comparison_df, left_title_dynamic, right_title_dynamic, mobile_mode=False)
    st.markdown(comparison_html, unsafe_allow_html=True)
