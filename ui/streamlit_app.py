import os
import sys

import streamlit as st

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from rwa_demo.workflow import DemoWorkflowService


st.set_page_config(page_title="RWA Policy-to-SQL Demo", layout="wide")
st.title("RWA Policy-to-SQL Demo")
st.caption("Global Finance + Treasury | Google-native | OBJECT_REF lineage")

service = DemoWorkflowService()

with st.sidebar:
    st.header("Operator")
    operator = st.text_input("User", value="finance.lead@hsbc.com")

tabs = st.tabs(
    [
        "1) Upload Policy PDF",
        "2) Generate SQL",
        "3) Approve + Execute SQL Agent",
        "4) View Tables",
    ]
)

with tabs[0]:
    st.subheader("Upload baseline or updated policy")
    existing_policy_id = st.text_input("Existing policy_id (optional for updates)")
    supersedes = st.text_input("Supersedes policy_version_id (optional)")
    uploaded = st.file_uploader("Policy PDF", type=["pdf"])
    if st.button("Upload policy", type="primary", disabled=uploaded is None):
        if uploaded is None:
            st.error("Upload a PDF first.")
        else:
            policy_id, policy_version_id, gcs_uri = service.upload_policy(
                uploaded_by=operator,
                filename=uploaded.name,
                file_bytes=uploaded.read(),
                existing_policy_id=existing_policy_id or None,
                supersedes_policy_version_id=supersedes or None,
            )
            st.success("Policy uploaded and registered.")
            st.write({"policy_id": policy_id, "policy_version_id": policy_version_id, "gcs_uri": gcs_uri})

with tabs[1]:
    st.subheader("Generate SQL from policy")
    policy_version_id = st.text_input("policy_version_id")
    if st.button("Generate SQL", disabled=not policy_version_id):
        sql_version_id, summary, sql = service.generate_sql(policy_version_id=policy_version_id)
        st.success("SQL generated and stored.")
        st.write({"sql_version_id": sql_version_id, "summary": summary})
        st.code(sql, language="sql")

with tabs[2]:
    st.subheader("Approve SQL and execute agent")
    c1, c2 = st.columns(2)
    with c1:
        sql_version_id = st.text_input("sql_version_id")
        if st.button("Approve SQL", disabled=not sql_version_id):
            service.approve_sql(sql_version_id=sql_version_id, approved_by=operator)
            st.success(f"Approved {sql_version_id}")
    with c2:
        policy_id = st.text_input("policy_id")
        policy_version_id = st.text_input("policy_version_id ")
        sql_version_id_exec = st.text_input("sql_version_id ")
        if st.button(
            "Run SQL Execution Agent",
            type="primary",
            disabled=not (policy_id and policy_version_id and sql_version_id_exec),
        ):
            run_id = service.execute_sql_agent(
                policy_id=policy_id,
                policy_version_id=policy_version_id,
                sql_version_id=sql_version_id_exec,
            )
            st.success(f"Execution completed. run_id={run_id}")

with tabs[3]:
    st.subheader("Inspect lineage + outputs")
    table = st.selectbox(
        "Table",
        [
            "policy_documents",
            "policy_extractions",
            "policy_sql_versions",
            "report_runs",
            "rwa_report_outputs",
        ],
    )
    if st.button("Refresh table"):
        rows = service.repo.list_table(table, limit=200)
        st.dataframe(rows, use_container_width=True)

