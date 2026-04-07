import json
import html
import re
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Blocking Review", layout="wide")

DB_URL = st.secrets.get("DB_URL")
if not DB_URL:
    st.error("Secret DB_URL fehlt.")
    st.stop()

engine = create_engine(DB_URL)
DECISION_OPTIONS = ["BLOCK_OK", "BLOCK_NOK", "UNSURE"]

# =========================================================
# PAGE CSS
# =========================================================
st.markdown(
    """
    <style>
    html, body, [data-testid="stAppViewContainer"], .main {
        height: 100vh !important;
        overflow: hidden !important;
    }

    [data-testid="stHeader"] {
        height: 0rem;
    }

    .block-container {
        padding-top: 0.8rem !important;
        padding-bottom: 0.4rem !important;
        max-width: 100% !important;
        height: 100vh !important;
        overflow: hidden !important;
    }

    [data-testid="stSidebar"] {
        overflow-y: auto !important;
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

    .top-score-label {
        font-size: 14px;
        color: #6c757d;
        margin-bottom: 2px;
    }

    .top-score-value {
        font-size: 34px;
        font-weight: 800;
        line-height: 1.1;
    }

    .compact-note {
        font-size: 13px;
        color: #6c757d;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# =========================================================
# DATA LOADING
# =========================================================
@st.cache_data(ttl=10)
def load_open_runs() -> pd.DataFrame:
    query = text("""
        select
            r.run_id,
            r.left_source,
            r.right_source,
            r.status,
            r.created_at,
            r.updated_at,
            count(c.pair_key) as open_case_count
        from review.review_runs r
        join review.review_cases c
            on r.run_id = c.run_id
        where c.status = 'open'
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
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=10)
def load_open_cases(run_id: str) -> pd.DataFrame:
    query = text("""
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
            status
        from review.review_cases
        where run_id = :run_id
          and status = 'open'
        order by score_total asc nulls last, pair_key asc
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"run_id": run_id})


@st.cache_data(ttl=10)
def load_all_cases_for_run(run_id: str) -> pd.DataFrame:
    query = text("""
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
            status
        from review.review_cases
        where run_id = :run_id
        order by score_total asc nulls last, pair_key asc
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"run_id": run_id})


# =========================================================
# HELPERS
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
    right_title: str
) -> pd.DataFrame:
    all_params = list(dict.fromkeys(list(left_dict.keys()) + list(right_dict.keys())))

    priority_params = ["key_lvl3"]
    remaining_params = [p for p in all_params if p not in priority_params]
    ordered_params = priority_params + remaining_params

    rows = []
    for param in ordered_params:
        left_val = left_dict.get(param, "")
        right_val = right_dict.get(param, "")

        status, diff_text, compare_type = compare_values(left_val, right_val, tolerance_pct)

        rows.append({
            "Parameter": str(param),
            left_title: "" if pd.isna(left_val) else str(left_val),
            right_title: "" if pd.isna(right_val) else str(right_val),
            "_status": status,
            "_diff": diff_text,
            "_compare_type": compare_type
        })

    df = pd.DataFrame(rows)

    status_rank = {
        "different": 0,
        "within_tolerance": 1,
        "normalized_equal": 2,
        "exact_equal": 3,
        "empty_equal": 4
    }

    df["_priority"] = df["Parameter"].apply(lambda x: 0 if x == "key_lvl3" else 1)
    df["_sort_rank"] = df["_status"].map(status_rank).fillna(99)
    df = df.sort_values(["_priority", "_sort_rank", "Parameter"], kind="stable").reset_index(drop=True)
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
    if status == "empty_equal":
        return "EMPTY"
    return status


def render_comparison_table_html(df: pd.DataFrame, left_title: str, right_title: str) -> str:
    def bg(status: str) -> str:
        if status == "different":
            return "#f8d7da"
        if status == "within_tolerance":
            return "#fff3cd"
        if status == "normalized_equal":
            return "#fff3cd"
        if status == "exact_equal":
            return "#d1e7dd"
        if status == "empty_equal":
            return "#f8f9fa"
        return "#ffffff"

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
        height: calc(100vh - 360px);
        min-height: 280px;
        max-height: calc(100vh - 360px);
        overflow-y: auto;
        overflow-x: auto;
        border: 1px solid #ddd;
        border-radius: 8px;
        box-sizing: border-box;
        background: white;
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


def show_popup_message(message: str, duration_ms: int = 900):
    text = html.escape(message)

    popup_html = f"""
    <div id="save-popup-overlay" style="
        position: fixed;
        inset: 0;
        z-index: 999998;
        background: rgba(128, 128, 128, 0.45);
        display: flex;
        align-items: center;
        justify-content: center;
        opacity: 1;
        transition: opacity 0.3s ease;
        backdrop-filter: blur(1px);
    ">
        <div id="save-popup" style="
            background: white;
            color: #222;
            padding: 16px 22px;
            border-radius: 12px;
            box-shadow: 0 12px 32px rgba(0,0,0,0.25);
            font-weight: 600;
            font-size: 15px;
            min-width: 180px;
            text-align: center;
        ">
            {text}
        </div>
    </div>
    <script>
        setTimeout(function() {{
            var overlay = window.parent.document.getElementById("save-popup-overlay");
            if (overlay) {{
                overlay.style.opacity = "0";
                setTimeout(function() {{
                    if (overlay) overlay.remove();
                }}, 350);
            }}
        }}, {duration_ms});
    </script>
    """
    st.components.v1.html(popup_html, height=0)


def save_decision(pair_row, decision: str, comment: str, reviewer: str):
    with engine.begin() as conn:
        conn.execute(text("""
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
        """), {
            "run_id": pair_row["run_id"],
            "pair_key": pair_row["pair_key"],
            "pair_id": pair_row["pair_id"],
            "left_id": pair_row["left_id"],
            "right_id": pair_row["right_id"],
            "decision": decision,
            "comment": comment,
            "reviewer": reviewer,
            "timestamp": datetime.utcnow(),
        })

        conn.execute(text("""
            update review.review_cases
            set status = 'reviewed',
                updated_at = now()
            where pair_key = :pair_key
              and run_id = :run_id
        """), {
            "pair_key": pair_row["pair_key"],
            "run_id": pair_row["run_id"],
        })

        remaining_open = conn.execute(text("""
            select count(*) as n
            from review.review_cases
            where run_id = :run_id
              and status = 'open'
        """), {
            "run_id": pair_row["run_id"],
        }).scalar_one()

        if remaining_open == 0:
            conn.execute(text("""
                update review.review_runs
                set status = 'reviewed',
                    updated_at = now()
                where run_id = :run_id
            """), {
                "run_id": pair_row["run_id"],
            })


def run_label(row) -> str:
    left_source = str(row.get("left_source", ""))
    right_source = str(row.get("right_source", ""))
    open_case_count = row.get("open_case_count", 0)
    return f"{left_source} vs {right_source} | offen: {open_case_count}"


def init_session_for_run(selected_run_id: str, all_cases_df: pd.DataFrame):
    current_signature = f"{selected_run_id}|{len(all_cases_df)}"
    if st.session_state.get("run_signature") != current_signature:
        st.session_state["run_signature"] = current_signature
        st.session_state["active_run_id"] = selected_run_id
        st.session_state["session_case_keys"] = all_cases_df["pair_key"].astype(str).tolist()
        st.session_state["session_total"] = len(all_cases_df)
        st.session_state["current_pair_key"] = None


def find_next_open_pair_key(session_case_keys, open_cases_df, current_pair_key=None):
    open_keys = set(open_cases_df["pair_key"].astype(str).tolist())
    if not open_keys:
        return None

    if current_pair_key is None:
        for key in session_case_keys:
            if key in open_keys:
                return key
        return None

    try:
        start_idx = session_case_keys.index(current_pair_key)
    except ValueError:
        start_idx = -1

    for i in range(start_idx + 1, len(session_case_keys)):
        if session_case_keys[i] in open_keys:
            return session_case_keys[i]

    for i in range(0, start_idx + 1):
        if session_case_keys[i] in open_keys:
            return session_case_keys[i]

    return None


def find_prev_open_pair_key(session_case_keys, open_cases_df, current_pair_key=None):
    open_keys = set(open_cases_df["pair_key"].astype(str).tolist())
    if not open_keys:
        return None

    if current_pair_key is None:
        for key in session_case_keys:
            if key in open_keys:
                return key
        return None

    try:
        start_idx = session_case_keys.index(current_pair_key)
    except ValueError:
        start_idx = len(session_case_keys)

    for i in range(start_idx - 1, -1, -1):
        if session_case_keys[i] in open_keys:
            return session_case_keys[i]

    for i in range(len(session_case_keys) - 1, start_idx - 1, -1):
        if session_case_keys[i] in open_keys:
            return session_case_keys[i]

    return None


def current_session_position(session_case_keys, current_pair_key):
    if current_pair_key in session_case_keys:
        return session_case_keys.index(current_pair_key) + 1
    return 1


# =========================================================
# APP START
# =========================================================
st.title("Blocking Review")

runs_df = load_open_runs()

if len(runs_df) == 0:
    st.success("Keine offenen Runs vorhanden.")
    st.stop()

# =========================================================
# SIDEBAR CONTROLS
# =========================================================
run_options = runs_df["run_id"].tolist()
run_label_map = {
    row["run_id"]: run_label(row)
    for _, row in runs_df.iterrows()
}

selected_run_id = st.sidebar.selectbox(
    "Review-Run auswählen",
    options=run_options,
    format_func=lambda x: run_label_map.get(x, x),
)

reviewer = st.sidebar.text_input(
    "Reviewer",
    value=st.session_state.get("reviewer_name", "user"),
    key="reviewer_name"
)

selected_run_row = runs_df[runs_df["run_id"] == selected_run_id].iloc[0]

all_cases_df = load_all_cases_for_run(selected_run_id)
open_cases_df = load_open_cases(selected_run_id)

init_session_for_run(selected_run_id, all_cases_df)

session_case_keys = st.session_state["session_case_keys"]
session_total = st.session_state["session_total"]

if len(open_cases_df) == 0:
    st.success("Dieser Run hat keine offenen Fälle mehr.")
    st.cache_data.clear()
    st.stop()

if (
    st.session_state.get("current_pair_key") is None
    or st.session_state.get("current_pair_key") not in set(open_cases_df["pair_key"].astype(str))
):
    st.session_state["current_pair_key"] = find_next_open_pair_key(
        session_case_keys=session_case_keys,
        open_cases_df=open_cases_df,
        current_pair_key=None
    )

current_pair_key = st.session_state["current_pair_key"]

if current_pair_key is None:
    st.success("Dieser Run hat keine offenen Fälle mehr.")
    st.cache_data.clear()
    st.stop()

pair_row = open_cases_df[
    open_cases_df["pair_key"].astype(str) == str(current_pair_key)
].iloc[0]

left_payload = parse_payload(pair_row["left_payload"])
right_payload = parse_payload(pair_row["right_payload"])

left_title_dynamic = str(pair_row.get("left_source", "Left"))
right_title_dynamic = str(pair_row.get("right_source", "Right"))

session_pair_number = current_session_position(session_case_keys, current_pair_key)
session_reviewed_count = session_total - len(open_cases_df)
progress_value = session_reviewed_count / session_total if session_total > 0 else 1.0

# =========================================================
# SIDEBAR STATUS
# =========================================================
st.sidebar.write(f"Session-Paar {session_pair_number} / {session_total}")
st.sidebar.progress(progress_value)
st.sidebar.write(f"Bereits bearbeitet: {session_reviewed_count}")
st.sidebar.write(f"Noch offen: {len(open_cases_df)}")
st.sidebar.write(f"Run ID: {selected_run_id}")

tolerance_pct = st.sidebar.number_input(
    "Toleranz in Prozent",
    min_value=0.0,
    value=5.0,
    step=0.5,
    help="Wenn zwei numerische Werte sich hoechstens um diesen Prozentwert unterscheiden, werden sie gelb markiert."
)

st.sidebar.markdown("**Farblegende**")
st.sidebar.markdown(
    """
    <div style="display:flex; gap:8px; align-items:center; margin-bottom:6px;">
      <div style="width:18px;height:18px;background:#f8d7da;border:1px solid #ccc;"></div><div>Rot = unterschiedlich</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; margin-bottom:6px;">
      <div style="width:18px;height:18px;background:#fff3cd;border:1px solid #ccc;"></div><div>Gelb = innerhalb Prozent-Toleranz / Text fast gleich</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; margin-bottom:6px;">
      <div style="width:18px;height:18px;background:#d1e7dd;border:1px solid #ccc;"></div><div>Gruen = exakt gleich</div>
    </div>
    <div style="display:flex; gap:8px; align-items:center; margin-bottom:6px;">
      <div style="width:18px;height:18px;background:#f8f9fa;border:1px solid #ccc;"></div><div>Grau = beide leer</div>
    </div>
    """,
    unsafe_allow_html=True
)

# =========================================================
# TOP INFO + DECISION
# =========================================================
header_left, header_right = st.columns([1.0, 2.2])

with header_left:
    score_val = pair_row["score_total"]
    conf_text = "-"
    if pd.notna(score_val):
        try:
            conf_text = f"{float(score_val):.4f}"
        except Exception:
            conf_text = str(score_val)

    st.markdown(
        f"""
        <div class="top-score-card">
            <div class="top-score-label">Confidence Score</div>
            <div class="top-score-value">{html.escape(conf_text)}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        f"""
        <div class="compact-note">
            Session-Paar {session_pair_number} / {session_total}<br>
            {html.escape(left_title_dynamic)} vs {html.escape(right_title_dynamic)}
        </div>
        """,
        unsafe_allow_html=True
    )

with header_right:
    st.markdown('<div class="review-section-title">Entscheidung</div>', unsafe_allow_html=True)

    decision_left, decision_right = st.columns([1.25, 1.0])

    with decision_left:
        decision = st.radio(
            "Ist diese Kombination im Blocking sinnvoll?",
            options=DECISION_OPTIONS,
            horizontal=True,
            format_func=lambda x: {
                "BLOCK_OK": "Blocking passt",
                "BLOCK_NOK": "Blocking passt nicht",
                "UNSURE": "Unklar"
            }.get(x, x)
        )

    with decision_right:
        comment = st.text_area("Kommentar", height=95)

    btn1, btn2, btn3 = st.columns([1, 1.5, 1.2])

    with btn1:
        if st.button("Zurueck", use_container_width=True):
            prev_key = find_prev_open_pair_key(
                session_case_keys=session_case_keys,
                open_cases_df=open_cases_df,
                current_pair_key=current_pair_key
            )
            if prev_key is not None:
                st.session_state["current_pair_key"] = prev_key
            st.rerun()

    with btn2:
        if st.button("Speichern + Weiter", use_container_width=True):
            current_key_before_save = current_pair_key

            save_decision(pair_row, decision, comment, reviewer)
            st.cache_data.clear()

            new_open_cases_df = load_open_cases(selected_run_id)
            next_key = find_next_open_pair_key(
                session_case_keys=session_case_keys,
                open_cases_df=new_open_cases_df,
                current_pair_key=current_key_before_save
            )

            st.session_state["current_pair_key"] = next_key

            try:
                st.toast("Gespeichert")
            except Exception:
                pass
            show_popup_message("Gespeichert", duration_ms=900)
            st.rerun()

    with btn3:
        if st.button("Weiter ohne Speichern", use_container_width=True):
            next_key = find_next_open_pair_key(
                session_case_keys=session_case_keys,
                open_cases_df=open_cases_df,
                current_pair_key=current_pair_key
            )
            if next_key is not None:
                st.session_state["current_pair_key"] = next_key
            st.rerun()

# =========================================================
# COMPARISON TABLE
# =========================================================
comparison_df = build_combined_display_df(
    left_payload,
    right_payload,
    tolerance_pct,
    left_title_dynamic,
    right_title_dynamic
)

comparison_html = render_comparison_table_html(
    comparison_df,
    left_title_dynamic,
    right_title_dynamic
)

st.markdown(comparison_html, unsafe_allow_html=True)