import difflib
import json
import os
import sys

import plotly.graph_objects as go
import streamlit as st

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from rwa_demo.workflow import DemoWorkflowService

# ---------------------------------------------------------------------------
# Page config & custom CSS
# ---------------------------------------------------------------------------
st.set_page_config(page_title="RWA Policy-to-SQL Demo", layout="wide", page_icon="🏦")

st.markdown(
    """
    <style>
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8f9fc 0%, #e8edf5 100%);
        border: 1px solid #d0d7e6;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.06);
    }
    [data-testid="stMetric"] label { font-size: 0.85rem; font-weight: 600; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 1.6rem; }
    .diff-add { background: #e6ffec; }
    .diff-del { background: #ffebe9; }
    .clause-badge {
        display: inline-block; border-radius: 4px; padding: 2px 8px;
        font-weight: 700; font-size: 0.8rem; margin-right: 6px; color: #fff;
    }
    .badge-threshold { background: #d63384; }
    .badge-mapping { background: #0d6efd; }
    .badge-calculation { background: #198754; }
    .badge-exclusion { background: #dc3545; }
    .badge-definition { background: #6f42c1; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("RWA Policy-to-SQL Demo")
st.caption("Global Finance + Treasury · Google-native · OBJECT_REF lineage · Vertex AI Agent")

service = DemoWorkflowService()

# ---------------------------------------------------------------------------
# Enhancement 1: Executive KPI Cards
# ---------------------------------------------------------------------------
try:
    metrics = service.repo.get_dashboard_metrics()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Policies Uploaded", metrics["total_policies"])
    k2.metric("Approved SQL Versions", metrics["approved_sql"])
    k3.metric("Successful Runs", metrics["successful_runs"])
    total_rwa = metrics["total_rwa"]
    k4.metric("Total RWA", f"${total_rwa / 1e6:,.1f}M" if total_rwa >= 1e6 else f"${total_rwa:,.0f}")
except Exception:
    st.info("Connect to BigQuery to see live KPI metrics.")

st.divider()

# ---------------------------------------------------------------------------
# Sidebar: Operator + Demo Mode
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Operator")
    operator = st.text_input("User", value="finance.lead@bank.com")

    st.divider()
    st.header("Demo Mode")
    demo_mode = st.toggle("One-click guided walkthrough", value=False)

    if demo_mode:
        st.markdown("---")
        st.markdown("### Demo Steps")
        steps = [
            "Act 1: Upload Baseline Policy",
            "Act 1: Generate SQL",
            "Act 1: Approve & Execute",
            "Act 2: Upload Updated Policy",
            "Act 2: Generate SQL",
            "Act 2: Approve & Execute",
            "Act 3: View Impact Dashboard",
            "Act 3: Explore Lineage",
        ]
        current_step = st.session_state.get("demo_step", 0)
        for i, step in enumerate(steps):
            if i < current_step:
                st.markdown(f"~~:white_check_mark: {step}~~")
            elif i == current_step:
                st.markdown(f"**:arrow_forward: {step}**")
            else:
                st.markdown(f":white_circle: {step}")

        if st.button("Reset Demo", type="secondary"):
            for key in list(st.session_state.keys()):
                if key.startswith("demo_"):
                    del st.session_state[key]
            st.rerun()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tabs = st.tabs(
    [
        "1) Upload Policy",
        "2) Generate SQL",
        "3) Approve & Execute",
        "4) SQL Diff",
        "5) Impact Dashboard",
        "6) Explainability",
        "7) Lineage & Audit",
    ]
)

# ---------------------------------------------------------------------------
# Tab 1 – Upload Policy PDF
# ---------------------------------------------------------------------------
with tabs[0]:
    st.subheader("Upload baseline or updated policy")
    existing_policy_id = st.text_input(
        "Existing policy_id (leave blank for new policy)",
        value=st.session_state.get("demo_policy_id", ""),
        key="upload_existing_pid",
    )
    supersedes = st.text_input(
        "Supersedes policy_version_id (optional)",
        value=st.session_state.get("demo_supersedes", ""),
        key="upload_supersedes",
    )
    uploaded = st.file_uploader("Policy PDF", type=["pdf"])

    if demo_mode:
        step = st.session_state.get("demo_step", 0)
        if step in (0, 3):
            st.info(
                "**Demo Mode** — Upload a policy PDF and click the button. "
                "IDs will be auto-saved for subsequent steps."
            )

    if st.button("Upload policy", type="primary", disabled=uploaded is None):
        if uploaded is None:
            st.error("Upload a PDF first.")
        else:
            with st.spinner("Uploading to Cloud Storage and registering in BigQuery..."):
                policy_id, policy_version_id, gcs_uri = service.upload_policy(
                    uploaded_by=operator,
                    filename=uploaded.name,
                    file_bytes=uploaded.read(),
                    existing_policy_id=existing_policy_id or None,
                    supersedes_policy_version_id=supersedes or None,
                )
            st.success("Policy uploaded and registered.")
            st.json({"policy_id": policy_id, "policy_version_id": policy_version_id, "gcs_uri": gcs_uri})

            # Save to session for demo mode and auto-fill
            st.session_state["demo_policy_id"] = policy_id
            st.session_state["demo_policy_version_id"] = policy_version_id
            if demo_mode:
                st.session_state["demo_step"] = st.session_state.get("demo_step", 0) + 1

# ---------------------------------------------------------------------------
# Tab 2 – Generate SQL (Enhancement 2: streaming progress)
# ---------------------------------------------------------------------------
with tabs[1]:
    st.subheader("Generate SQL from policy")
    gen_pvid = st.text_input(
        "policy_version_id",
        value=st.session_state.get("demo_policy_version_id", ""),
        key="gen_pvid",
    )

    if st.button("Generate SQL", type="primary", disabled=not gen_pvid):
        with st.status("Generating SQL via Vertex AI Agent...", expanded=True) as status:
            result = None
            for step in service.generate_sql_phased(gen_pvid):
                phase = step[0]
                if phase == "fetch_policy":
                    st.write(":mag: " + step[1])
                elif phase == "fetch_policy_done":
                    st.write(":white_check_mark: " + step[1])
                elif phase == "schema_snapshot":
                    st.write(":card_file_box: " + step[1])
                elif phase == "schema_snapshot_done":
                    st.write(":white_check_mark: " + step[1])
                elif phase == "agent_generating":
                    st.write(":robot_face: " + step[1])
                elif phase == "agent_done":
                    st.write(":white_check_mark: " + step[1])
                elif phase == "saving":
                    st.write(":floppy_disk: " + step[1])
                elif phase == "complete":
                    result = step
            status.update(label="SQL generation complete!", state="complete", expanded=False)

        if result:
            _, sql_version_id, summary, generated_sql, agent_trace = result
            st.session_state["demo_sql_version_id"] = sql_version_id
            st.success(f"sql_version_id: `{sql_version_id}`")
            st.markdown(f"**Summary:** {summary}")

            # Show clause citations if available
            citations = agent_trace.get("clause_citations", [])
            if citations and isinstance(citations, list) and isinstance(citations[0], dict):
                st.markdown("#### Extracted Policy Clauses")
                for c in citations:
                    ctype = c.get("clause_type", "definition")
                    badge = f'<span class="clause-badge badge-{ctype}">{c.get("clause_id", "?")}</span>'
                    st.markdown(
                        f'{badge} **{ctype.title()}**: {c.get("clause_text", "")} '
                        f'→ *{c.get("sql_section", "")}*',
                        unsafe_allow_html=True,
                    )

            st.markdown("#### Generated SQL")
            st.code(generated_sql, language="sql")

            if demo_mode:
                st.session_state["demo_step"] = st.session_state.get("demo_step", 0) + 1

# ---------------------------------------------------------------------------
# Tab 3 – Approve + Execute
# ---------------------------------------------------------------------------
with tabs[2]:
    st.subheader("Approve SQL and execute agent")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Approve")
        approve_svid = st.text_input(
            "sql_version_id",
            value=st.session_state.get("demo_sql_version_id", ""),
            key="approve_svid",
        )
        if st.button("Approve SQL", disabled=not approve_svid):
            service.approve_sql(sql_version_id=approve_svid, approved_by=operator)
            st.success(f"Approved `{approve_svid}`")
    with c2:
        st.markdown("#### Execute")
        exec_pid = st.text_input(
            "policy_id",
            value=st.session_state.get("demo_policy_id", ""),
            key="exec_pid",
        )
        exec_pvid = st.text_input(
            "policy_version_id",
            value=st.session_state.get("demo_policy_version_id", ""),
            key="exec_pvid",
        )
        exec_svid = st.text_input(
            "sql_version_id",
            value=st.session_state.get("demo_sql_version_id", ""),
            key="exec_svid",
        )
        if st.button(
            "Run SQL Execution Agent",
            type="primary",
            disabled=not (exec_pid and exec_pvid and exec_svid),
        ):
            with st.spinner("Agent executing SQL against BigQuery..."):
                run_id = service.execute_sql_agent(
                    policy_id=exec_pid,
                    policy_version_id=exec_pvid,
                    sql_version_id=exec_svid,
                )
            st.success(f"Execution completed. `run_id={run_id}`")
            st.session_state["demo_run_id"] = run_id
            # Track baseline vs updated run
            if "demo_run_baseline" not in st.session_state:
                st.session_state["demo_run_baseline"] = run_id
            else:
                st.session_state["demo_run_updated"] = run_id

            if demo_mode:
                st.session_state["demo_step"] = st.session_state.get("demo_step", 0) + 1

# ---------------------------------------------------------------------------
# Tab 4 – SQL Diff Viewer (Enhancement 3)
# ---------------------------------------------------------------------------
with tabs[3]:
    st.subheader("SQL Version Diff")
    st.caption("Compare generated SQL between two policy versions")

    diff_pid = st.text_input(
        "policy_id (to list all SQL versions)",
        value=st.session_state.get("demo_policy_id", ""),
        key="diff_pid",
    )

    if diff_pid and st.button("Load SQL versions", key="load_sql_versions"):
        versions = service.repo.get_sql_versions_for_policy(diff_pid)
        st.session_state["diff_versions"] = versions

    versions = st.session_state.get("diff_versions", [])
    if versions:
        labels = [f"{v['sql_version_id']} ({v['policy_version_id']})" for v in versions]
        col_a, col_b = st.columns(2)
        with col_a:
            idx_a = st.selectbox("Baseline SQL", range(len(labels)), format_func=lambda i: labels[i], key="diff_a")
        with col_b:
            idx_b = st.selectbox("Updated SQL", range(len(labels)), format_func=lambda i: labels[i], index=min(1, len(labels) - 1), key="diff_b")

        sql_a = versions[idx_a]["generated_sql"]
        sql_b = versions[idx_b]["generated_sql"]

        diff = difflib.HtmlDiff(wrapcolumn=80)
        diff_html = diff.make_table(
            sql_a.splitlines(),
            sql_b.splitlines(),
            fromdesc=labels[idx_a],
            todesc=labels[idx_b],
            context=True,
            numlines=3,
        )
        # Style the diff table
        styled = f"""
        <style>
        .diff_header {{ background-color: #f1f3f5; font-weight: bold; }}
        .diff_next {{ background-color: #e9ecef; }}
        .diff_add {{ background-color: #e6ffec; }}
        .diff_chg {{ background-color: #fff3cd; }}
        .diff_sub {{ background-color: #ffebe9; }}
        table.diff {{ font-family: monospace; font-size: 0.8rem; border-collapse: collapse; width: 100%; }}
        table.diff td {{ padding: 2px 6px; border: 1px solid #dee2e6; }}
        </style>
        {diff_html}
        """
        st.markdown(styled, unsafe_allow_html=True)

        # Summary stats
        lines_a = sql_a.splitlines()
        lines_b = sql_b.splitlines()
        added = sum(1 for line in difflib.unified_diff(lines_a, lines_b) if line.startswith("+") and not line.startswith("+++"))
        removed = sum(1 for line in difflib.unified_diff(lines_a, lines_b) if line.startswith("-") and not line.startswith("---"))
        st.caption(f"Lines added: **{added}** · Lines removed: **{removed}** · Net: **{added - removed:+d}**")
    elif not diff_pid:
        st.info("Enter a policy_id and click Load to see SQL version diffs.")

# ---------------------------------------------------------------------------
# Tab 5 – RWA Delta Impact Dashboard (Enhancement 4)
# ---------------------------------------------------------------------------
with tabs[4]:
    st.subheader("RWA Impact Dashboard")
    st.caption("Compare RWA outputs between two report runs")

    try:
        runs = service.repo.list_report_runs()
    except Exception:
        runs = []

    if not runs:
        st.info("No report runs yet. Execute SQL from the previous tabs first.")
    else:
        run_labels = {r["run_id"]: f"{r['run_id']} ({r['run_status']}) — {r.get('policy_version_id', '')}" for r in runs}
        run_ids = list(run_labels.keys())

        col_r1, col_r2 = st.columns(2)
        with col_r1:
            baseline_run = st.selectbox(
                "Baseline Run",
                run_ids,
                format_func=lambda x: run_labels[x],
                index=min(0, len(run_ids) - 1),
                key="impact_baseline",
            )
        with col_r2:
            updated_run = st.selectbox(
                "Updated Run",
                run_ids,
                format_func=lambda x: run_labels[x],
                index=min(1, len(run_ids) - 1) if len(run_ids) > 1 else 0,
                key="impact_updated",
            )

        if st.button("Compare Runs", type="primary", key="compare_runs"):
            comparison = service.repo.get_rwa_comparison(baseline_run, updated_run)
            if not comparison:
                st.warning("No RWA output data found for the selected runs.")
            else:
                st.session_state["impact_data"] = comparison

        data = st.session_state.get("impact_data", [])
        if data:
            # KPI summary
            total_baseline = sum(float(r["rwa_baseline"]) for r in data)
            total_updated = sum(float(r["rwa_updated"]) for r in data)
            total_delta = total_updated - total_baseline
            pct_change = (total_delta / total_baseline * 100) if total_baseline else 0

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Baseline RWA", f"${total_baseline / 1e6:,.2f}M")
            m2.metric("Updated RWA", f"${total_updated / 1e6:,.2f}M")
            m3.metric("Absolute Delta", f"${total_delta / 1e6:,.2f}M", delta=f"{total_delta / 1e6:+,.2f}M")
            m4.metric("% Change", f"{pct_change:+.1f}%", delta=f"{pct_change:+.1f}%")

            st.markdown("---")

            # Grouped bar chart
            labels = [f"{r['portfolio']}\n{r['risk_bucket']}" for r in data]
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                name="Baseline",
                x=labels,
                y=[float(r["rwa_baseline"]) for r in data],
                marker_color="#6c8ebf",
            ))
            fig_bar.add_trace(go.Bar(
                name="Updated",
                x=labels,
                y=[float(r["rwa_updated"]) for r in data],
                marker_color="#d4a373",
            ))
            fig_bar.update_layout(
                barmode="group",
                title="RWA by Portfolio / Risk Bucket",
                yaxis_title="RWA Amount",
                template="plotly_white",
                height=400,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

            # Waterfall chart
            waterfall_labels = [f"{r['portfolio']} / {r['risk_bucket']}" for r in data]
            waterfall_values = [float(r["rwa_delta"]) for r in data]
            fig_wf = go.Figure(go.Waterfall(
                name="Delta",
                orientation="v",
                x=waterfall_labels + ["Total"],
                y=waterfall_values + [total_delta],
                measure=["relative"] * len(data) + ["total"],
                connector={"line": {"color": "#999"}},
                increasing={"marker": {"color": "#dc3545"}},
                decreasing={"marker": {"color": "#198754"}},
                totals={"marker": {"color": "#0d6efd"}},
            ))
            fig_wf.update_layout(
                title="RWA Delta Waterfall (Impact of Policy Change)",
                yaxis_title="RWA Delta",
                template="plotly_white",
                height=400,
            )
            st.plotly_chart(fig_wf, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 6 – Clause-to-SQL Explainability (Enhancement 5)
# ---------------------------------------------------------------------------
with tabs[5]:
    st.subheader("Clause-to-SQL Explainability")
    st.caption("See how policy clauses map to generated SQL sections")

    explain_pvid = st.text_input(
        "policy_version_id",
        value=st.session_state.get("demo_policy_version_id", ""),
        key="explain_pvid",
    )

    if explain_pvid and st.button("Load Explainability", key="load_explain"):
        details = service.repo.get_extraction_details(explain_pvid)
        if details:
            st.session_state["explain_data"] = details
        else:
            st.warning("No extraction data found for this policy version.")

    details = st.session_state.get("explain_data")
    if details:
        left, right = st.columns([1, 1])

        # Parse clause citations from agent trace
        agent_trace = details.get("agent_trace", {})
        if isinstance(agent_trace, str):
            try:
                agent_trace = json.loads(agent_trace)
            except (json.JSONDecodeError, TypeError):
                agent_trace = {}

        citations = agent_trace.get("clause_citations", [])
        generated_sql = details.get("generated_sql", "")

        with left:
            st.markdown("#### Policy Clauses")
            if citations and isinstance(citations, list):
                for c in citations:
                    if isinstance(c, dict):
                        cid = c.get("clause_id", "?")
                        ctype = c.get("clause_type", "definition")
                        badge = f'<span class="clause-badge badge-{ctype}">{cid}</span>'
                        st.markdown(
                            f'{badge} **{ctype.title()}**',
                            unsafe_allow_html=True,
                        )
                        st.markdown(f"> {c.get('clause_text', 'N/A')}")
                        st.caption(f"SQL section: {c.get('sql_section', 'N/A')}")
                        st.markdown("---")
                    else:
                        st.write(f"- {c}")
            else:
                st.info("No structured clause citations available. The agent returned plain text citations.")
                if isinstance(citations, list):
                    for c in citations:
                        st.write(f"- {c}")

        with right:
            st.markdown("#### Generated SQL")
            if generated_sql:
                st.code(generated_sql, language="sql")
            else:
                st.info("No generated SQL found.")

# ---------------------------------------------------------------------------
# Tab 7 – Lineage & Audit (Enhancement 6)
# ---------------------------------------------------------------------------
with tabs[6]:
    st.subheader("Lineage & Audit Explorer")
    st.caption("Trace any output back to its source policy")

    # Lineage graph section
    lineage_run = st.text_input(
        "run_id",
        value=st.session_state.get("demo_run_id", ""),
        key="lineage_run",
    )

    if lineage_run and st.button("Show Lineage", key="show_lineage"):
        chain = service.repo.get_lineage_chain(lineage_run)
        if chain:
            st.session_state["lineage_chain"] = chain
        else:
            st.warning("No lineage data found for this run.")

    chain = st.session_state.get("lineage_chain")
    if chain:
        # Mermaid lineage graph
        pid = chain.get("policy_id", "?")
        pvid = chain.get("policy_version_id", "?")
        svid = chain.get("sql_version_id", "?")
        rid = chain.get("run_id", "?")
        run_status = chain.get("run_status", "?")
        policy_status = chain.get("policy_status", "?")
        val_status = chain.get("validation_status", "?")
        approved_by = chain.get("approved_by", "N/A")

        mermaid = f"""```mermaid
graph LR
    PDF["📄 Policy PDF<br/>{pid}"] --> VER["📋 Version<br/>{pvid}<br/>Status: {policy_status}"]
    VER --> EXT["🔍 Extraction<br/>Model: {chain.get('model_version', 'N/A')}"]
    EXT --> SQL["💻 SQL<br/>{svid}<br/>{val_status}"]
    SQL --> APPROVE["✅ Approved by<br/>{approved_by}"]
    APPROVE --> RUN["⚡ Run<br/>{rid}<br/>{run_status}"]
    RUN --> OUT["📊 RWA Outputs"]

    style PDF fill:#dbeafe,stroke:#3b82f6
    style VER fill:#dbeafe,stroke:#3b82f6
    style EXT fill:#fef3c7,stroke:#f59e0b
    style SQL fill:#d1fae5,stroke:#10b981
    style APPROVE fill:#d1fae5,stroke:#10b981
    style RUN fill:#ede9fe,stroke:#8b5cf6
    style OUT fill:#fce7f3,stroke:#ec4899
```"""
        st.markdown(mermaid)

        # Detail cards
        st.markdown("---")
        st.markdown("#### Lineage Details")
        d1, d2, d3 = st.columns(3)
        with d1:
            st.markdown("**Policy**")
            st.write(f"- **ID:** `{pid}`")
            st.write(f"- **Version:** `{pvid}`")
            st.write(f"- **Uploaded by:** {chain.get('uploaded_by', 'N/A')}")
            st.write(f"- **Uploaded at:** {chain.get('uploaded_at', 'N/A')}")
            st.write(f"- **GCS:** `{chain.get('gcs_uri', 'N/A')}`")
        with d2:
            st.markdown("**SQL Generation**")
            st.write(f"- **SQL Version:** `{svid}`")
            st.write(f"- **Validation:** {val_status}")
            st.write(f"- **Approved by:** {approved_by}")
            st.write(f"- **Approved at:** {chain.get('approved_at', 'N/A')}")
            st.write(f"- **Extraction model:** {chain.get('model_version', 'N/A')}")
        with d3:
            st.markdown("**Execution**")
            st.write(f"- **Run ID:** `{rid}`")
            st.write(f"- **Status:** {run_status}")
            st.write(f"- **Started:** {chain.get('run_started', 'N/A')}")
            st.write(f"- **Ended:** {chain.get('run_ended', 'N/A')}")

    # Raw table browser (kept from original)
    st.markdown("---")
    st.markdown("#### Raw Table Browser")
    table = st.selectbox(
        "Table",
        [
            "policy_documents",
            "policy_extractions",
            "policy_sql_versions",
            "report_runs",
            "rwa_report_outputs",
        ],
        key="raw_table",
    )
    if st.button("Refresh table", key="refresh_raw"):
        rows = service.repo.list_table(table, limit=200)
        st.dataframe(rows, use_container_width=True)
