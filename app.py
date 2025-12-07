# app.py
import io
from typing import List, Tuple
import os
import pandas as pd
import streamlit as st

from src.parsing.cibc_pdf_parser import extract_cibc_from_filelike
from src.storage.db import init_db, upsert_transactions, upsert_payments

st.set_page_config(page_title="Privacy-First Expense Tracker (CIBC MVP)", layout="wide")

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Upload CIBC PDFs")
    files = st.file_uploader(
        "Drop one or multiple statement PDFs",
        type=["pdf"],
        accept_multiple_files=True
    )
    st.caption("We parse only the Payments and New Charges tables. Nothing leaves your machine.")

    st.divider()
    st.header("Database")
    st.caption("Uses env vars: POSTGRES_HOST/PORT/DB/USER/PASSWORD (defaults: localhost:5432, personal_finance_tracker_db, user/123)")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Initialize DB (schema + views)"):
            try:
                init_db()
                st.success("DB initialized (schema 'expense', tables, indexes, views).")
            except Exception as e:
                st.error(f"Init failed: {e}")
    with col_b:
        st.write("")  # spacing

# ---------- Helpers ----------
@st.cache_data(show_spinner=False)
def parse_many(pdf_files: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_pays, all_txns = [], []
    for item in pdf_files:
        fname = item["name"]
        b = item["bytes"]
        try:
            buf = io.BytesIO(b)  # create a fresh buffer per file
            pays, txns = extract_cibc_from_filelike(buf)
            pays["statement_file"] = fname
            txns["statement_file"] = fname
            all_pays.append(pays)
            all_txns.append(txns)
        except Exception as e:
            st.error(f"Failed to parse {fname}: {e}")
            # keep pipeline stable even on one bad file
            all_pays.append(pd.DataFrame(columns=["trans_date","post_date","description","amount","source","statement_file"]))
            all_txns.append(pd.DataFrame(columns=["trans_date","post_date","description","category","amount","location","source","statement_file"]))
    pays_df = pd.concat(all_pays, ignore_index=True) if all_pays else pd.DataFrame()
    txns_df = pd.concat(all_txns, ignore_index=True) if all_txns else pd.DataFrame()
    if not txns_df.empty:
        txns_df = txns_df.sort_values(["trans_date","post_date","description"], kind="stable")
    if not pays_df.empty:
        pays_df = pays_df.sort_values(["trans_date","post_date","description"], kind="stable")
    return pays_df, txns_df

def download_df_button(df: pd.DataFrame, label: str, filename: str):
    if df.empty:
        st.button(label, disabled=True)
        return
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label, data=csv, file_name=filename, mime="text/csv")

# ---------- Header ----------
st.title("Privacy-First Expense Tracker — CIBC PDF Parser (No DB)")

st.markdown(
    """
    1) Upload one or more **CIBC statements (PDFs)** →  
    2) Review **Payments** & **Transactions** tables →  
    3) Inspect **quick charts** →  
    4) **Download CSVs** or **Save to Postgres**.  
    """
)

# ---------- Main ----------
if not files:
    st.info("Upload PDF(s) in the sidebar to begin.")
    st.stop()

# Keep original bytes for cache stability
uploads = [{"name": f.name, "bytes": f.getvalue()} for f in files]

with st.spinner("Parsing statements..."):
    payments_df, txns_df = parse_many(uploads)

tab1, tab2, tab3, tab4 = st.tabs(["Transactions", "Payments", "Charts", "Save to DB"])

# ---------- Transactions Tab ----------
with tab1:
    st.subheader("Transactions (Your new charges and credits)")
    if txns_df.empty:
        st.warning("No transactions found in uploaded PDFs.")
    else:
        col1, col2, col3 = st.columns(3)
        with col1:
            min_date = pd.to_datetime(txns_df["trans_date"].min()) if not txns_df.empty else None
            max_date = pd.to_datetime(txns_df["trans_date"].max()) if not txns_df.empty else None
            date_rng = st.date_input(
                "Filter by Transaction Date",
                value=(min_date.date(), max_date.date())
            ) if min_date is not None and max_date is not None else None
        with col2:
            cats = sorted([c for c in txns_df["category"].dropna().unique().tolist() if c != ""])
            sel_cats = st.multiselect("Categories", cats, default=cats)
        with col3:
            q = st.text_input("Search description (contains)", "")

        view = txns_df.copy()
        if date_rng:
            start = pd.to_datetime(date_rng[0])
            end = pd.to_datetime(date_rng[1]) + pd.Timedelta(days=1)
            view = view[(view["trans_date"] >= start) & (view["trans_date"] < end)]
        if sel_cats:
            view = view[view["category"].isin(sel_cats)]
        if q:
            view = view[view["description"].str.contains(q, case=False, na=False)]

        st.caption(f"Rows: {len(view):,}  |  Total Amount: ${view['amount'].sum():,.2f}")
        st.dataframe(
            view[["trans_date","post_date","description","location","category","amount","statement_file"]],
            use_container_width=True,
            hide_index=True
        )
        download_df_button(view, "Download Transactions CSV", "transactions_parsed.csv")

# ---------- Payments Tab ----------
with tab2:
    st.subheader("Payments")
    if payments_df.empty:
        st.info("No payments found.")
    else:
        st.caption(f"Rows: {len(payments_df):,}  |  Total: ${payments_df['amount'].sum():,.2f}")
        st.dataframe(
            payments_df[["trans_date","post_date","description","amount","statement_file"]],
            use_container_width=True,
            hide_index=True
        )
        download_df_button(payments_df, "Download Payments CSV", "payments_parsed.csv")

# ---------- Charts Tab ----------
with tab3:
    st.subheader("Quick Charts (exploratory)")
    if txns_df.empty:
        st.info("Upload statements with transactions to see charts.")
    else:
        # Daily spend (line)
        daily = (
            txns_df.assign(day=txns_df["trans_date"].dt.date)
                   .groupby("day", as_index=False)["amount"].sum()
                   .sort_values("day")
        )
        st.markdown("**Daily Spend (line)**")
        st.line_chart(data=daily.set_index("day"), y="amount", use_container_width=True)

        # Spend by Category (bar)
        cat_tot = (
            txns_df.groupby("category", as_index=False)["amount"].sum()
                   .sort_values("amount", ascending=False)
        )
        st.markdown("**Spend by Category (bar)**")
        st.bar_chart(data=cat_tot.set_index("category"), use_container_width=True)

        # Top Merchants (naive)
        st.markdown("**Top Merchants (naive)**")
        prov_tokens = {"ON","QC","BC","AB","MB","SK","NB","NS","NL","PE","YT","NT","NU"}
        def rough_merchant(desc: str) -> str:
            toks = str(desc).split()
            for i, t in enumerate(toks):
                if t.upper() in prov_tokens:
                    return " ".join(toks[:i]).strip()
            return desc
        top = (
            txns_df.assign(merchant=txns_df["description"].map(rough_merchant))
                   .groupby("merchant", as_index=False)["amount"].sum()
                   .sort_values("amount", ascending=False)
                   .head(15)
        )
        st.dataframe(top.rename(columns={"amount": "total_spend"}), use_container_width=True, hide_index=True)

# ---------- Save to DB Tab ----------
with tab4:
    st.subheader("Save parsed data to Postgres")
    st.caption("Schema: **expense**. Duplicates are skipped via a deterministic hash (natural_key) with ON CONFLICT DO NOTHING.")

    colA, colB = st.columns(2)
    with colA:
        if st.button("Upsert Transactions → DB", disabled=txns_df.empty):
            try:
                ins, skip = upsert_transactions(txns_df)
                st.success(f"Transactions: inserted {ins:,}, skipped {skip:,} (duplicates).")
            except Exception as e:
                st.error(f"Upsert transactions failed: {e}")
    with colB:
        if st.button("Upsert Payments → DB", disabled=payments_df.empty):
            try:
                ins, skip = upsert_payments(payments_df)
                st.success(f"Payments: inserted {ins:,}, skipped {skip:,} (duplicates).")
            except Exception as e:
                st.error(f"Upsert payments failed: {e}")

    st.info(
        "Tip: If you changed DB creds, set env vars POSTGRES_HOST/PORT/DB/USER/PASSWORD and restart the app.\n"
        "Default: localhost:5432 / personal_finance_tracker_db / user / 123"
    )