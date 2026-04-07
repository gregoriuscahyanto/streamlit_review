import json
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Blocking Review", layout="wide")

DB_URL = st.secrets["DB_URL"]
RUN_ID = st.secrets["RUN_ID"]

engine = create_engine(DB_URL)

DECISION_OPTIONS = ["BLOCK_OK", "BLOCK_NOK", "UNSURE"]


@st.cache_data(ttl=10)
def load_open_cases(run_id: str) -> pd.DataFrame:
    query = text("""
        select
            pair_key,
            run_id,
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
    return dict(payload)


def build_compare_df(left_dict: dict, right_dict: dict) -> pd.DataFrame:
    all_keys = sorted(set(left_dict.keys()).union(set(right_dict.keys())))
    rows = []

    for key in all_keys:
        left_val = left_dict.get(key, "")
        right_val = right_dict.get(key, "")

        left_txt = "" if left_val is None else str(left_val)
        right_txt = "" if right_val is None else str(right_val)

        if left_txt == right_txt:
            status = "EQ"
        elif left_txt.strip().lower() == right_txt.strip().lower():
            status = "TEXT_EQ"
        else:
            status = "DIFF"

        rows.append({
            "Parameter": key,
            "Left": left_txt,
            "Right": right_txt,
            "Status": status,
        })

    return pd.DataFrame(rows)


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
            "pair_id": pair_row["pair_key"],
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


st.title("Blocking Review")

st.write(f"**Run ID:** {RUN_ID}")

cases_df = load_open_cases(RUN_ID)

if "idx" not in st.session_state:
    st.session_state["idx"] = 0

if len(cases_df) == 0:
    st.success("Keine offenen Review-Fälle mehr vorhanden.")
    st.stop()

st.session_state["idx"] = max(0, min(st.session_state["idx"], len(cases_df) - 1))
idx = st.session_state["idx"]

pair_row = cases_df.iloc[idx]

left_payload = parse_payload(pair_row["left_payload"])
right_payload = parse_payload(pair_row["right_payload"])

compare_df = build_compare_df(left_payload, right_payload)

st.subheader(f"Fall {idx + 1} / {len(cases_df)}")

score_val = pair_row["score_total"]
if pd.notna(score_val):
    st.metric("Score", f"{float(score_val):.4f}")
else:
    st.metric("Score", "-")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Left")
    st.write(f"ID: {pair_row['left_id']}")

with col2:
    st.markdown("### Right")
    st.write(f"ID: {pair_row['right_id']}")

st.dataframe(compare_df, use_container_width=True, hide_index=True)

st.markdown("### Entscheidung")

reviewer = st.text_input("Reviewer", value="user")
decision = st.radio(
    "Ist diese Kombination im Blocking sinnvoll?",
    DECISION_OPTIONS,
    horizontal=True,
)
comment = st.text_area("Kommentar")

btn1, btn2, btn3 = st.columns(3)

with btn1:
    if st.button("Zurueck", use_container_width=True):
        st.session_state["idx"] = max(0, st.session_state["idx"] - 1)
        st.rerun()

with btn2:
    if st.button("Speichern + Weiter", use_container_width=True):
        save_decision(pair_row, decision, comment, reviewer)
        st.cache_data.clear()
        st.session_state["idx"] = min(st.session_state["idx"], max(0, len(cases_df) - 2))
        st.success("Gespeichert")
        st.rerun()

with btn3:
    if st.button("Weiter ohne Speichern", use_container_width=True):
        st.session_state["idx"] = min(len(cases_df) - 1, st.session_state["idx"] + 1)
        st.rerun()