import json
from pathlib import Path

import pandas as pd
import streamlit as st

DEFAULT_CSV = "andhra_damage_timeline.csv"
DEFAULT_REVIEWED_CSV = "andhra_damage_reviewed.csv"
DEFAULT_REVIEW_SUMMARY = "andhra_damage_review_summary.json"


def load_data(path: str) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        st.error(f"Input file not found: {path}. Run crawler first.")
        st.stop()
    df = pd.read_csv(csv_path)
    for col in [
        "needs_review",
        "include_in_total",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna(True).astype(bool)
    if "reviewer_amount_in_inr" not in df.columns:
        df["reviewer_amount_in_inr"] = df.get("extracted_amount_in_inr")
    if "reviewer_notes" not in df.columns:
        df["reviewer_notes"] = ""
    return df


def main() -> None:
    st.set_page_config(page_title="Incident Amount Review", layout="wide")
    st.title("Andhra Property Damage Review Dashboard")
    st.caption("Verify, edit, and approve incident amounts before final totals.")

    with st.sidebar:
        st.subheader("Files")
        input_csv = st.text_input("Input timeline CSV", DEFAULT_CSV)
        reviewed_csv = st.text_input("Reviewed CSV output", DEFAULT_REVIEWED_CSV)
        review_summary = st.text_input("Review summary JSON output", DEFAULT_REVIEW_SUMMARY)

    df = load_data(input_csv)
    st.write(f"Loaded {len(df)} incidents")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rows", len(df))
    with col2:
        with_amount = int(df["reviewer_amount_in_inr"].notna().sum())
        st.metric("Rows With Amount", with_amount)
    with col3:
        current_total = float(df.loc[df["include_in_total"], "reviewer_amount_in_inr"].fillna(0).sum())
        st.metric("Current Included Total (INR)", f"{current_total:,.2f}")
    if "incident_confidence_score" in df.columns:
        st.caption(
            f"Average confidence score: {float(df['incident_confidence_score'].fillna(0).mean()):.3f}"
        )

    show_review_only = st.checkbox("Show only rows still needing review", value=True)
    grid_df = df.copy()
    if show_review_only and "needs_review" in grid_df.columns:
        grid_df = grid_df[grid_df["needs_review"] == True]  # noqa: E712

    editable_columns = [
        "incident_id",
        "date",
        "title",
        "source_connector",
        "district_tag",
        "source_quality_score",
        "extraction_confidence_score",
        "incident_confidence_score",
        "duplicate_count",
        "extracted_amount_in_inr",
        "reviewer_amount_in_inr",
        "include_in_total",
        "needs_review",
        "reviewer_notes",
        "source_url",
        "duplicate_urls",
    ]
    existing_columns = [c for c in editable_columns if c in grid_df.columns]
    st.subheader("Review Table")
    edited_view = st.data_editor(
        grid_df[existing_columns],
        use_container_width=True,
        num_rows="fixed",
        hide_index=True,
        key="review_editor",
    )

    if st.button("Apply edits to full dataset"):
        if "incident_id" not in df.columns or "incident_id" not in edited_view.columns:
            st.error("incident_id column is required for safe merge of edits.")
            st.stop()
        merged = df.set_index("incident_id")
        updates = edited_view.set_index("incident_id")
        for col in updates.columns:
            merged.loc[updates.index, col] = updates[col]
        df = merged.reset_index()
        st.session_state["working_df"] = df
        st.success("Edits applied in memory.")

    if "working_df" in st.session_state:
        df = st.session_state["working_df"]

    save_col1, save_col2 = st.columns(2)
    with save_col1:
        if st.button("Save reviewed CSV"):
            df.to_csv(reviewed_csv, index=False)
            st.success(f"Saved: {reviewed_csv}")
    with save_col2:
        if st.button("Save review summary JSON"):
            total = float(df.loc[df["include_in_total"], "reviewer_amount_in_inr"].fillna(0).sum())
            summary = {
                "record_count": int(len(df)),
                "included_records": int(df["include_in_total"].fillna(False).sum()),
                "pending_review_records": int(df["needs_review"].fillna(False).sum()),
                "reviewed_total_estimated_damage_in_inr": round(total, 2),
                "average_incident_confidence_score": round(
                    float(df["incident_confidence_score"].fillna(0).mean()), 3
                ) if "incident_confidence_score" in df.columns else None,
            }
            Path(review_summary).write_text(json.dumps(summary, indent=2), encoding="utf-8")
            st.success(f"Saved: {review_summary}")


if __name__ == "__main__":
    main()
