import csv
import difflib
import io
import json
import os
import sys
import zipfile

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
st.set_page_config(page_title="HSBC & Google Cloud RWA Intelligence Platform", layout="wide", page_icon="🏦")

st.markdown(
    """
    <style>
    [data-testid="stMetric"] {
        background-color: rgba(128, 128, 128, 0.1);
        border: 1px solid rgba(128, 128, 128, 0.2);
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
    
    /* Chrome Tabs Style for Dashboard Buttons */
    div.stButton > button {
        min-height: 52px; /* Enforces equal height */
        border-radius: 12px 12px 0 0 !important;
        border: 1px solid #dfe1e5 !important;
        border-bottom: none !important;
        background-color: #f1f3f4 !important;
        color: #3c4043 !important;
        font-weight: 500 !important;
        font-size: 0.85rem !important;
        padding: 5px 10px !important;
        width: 100% !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
    }
    div.stButton > button:hover {
        background-color: #e8eaed !important;
    }
    
    /* Active Tab Style (using Primary Button trait) */
    div.stButton > button[kind="primary"], button[data-testid="stBaseButton-primary"] {
        background-color: #ffffff !important;
        color: #1a73e8 !important;
        border-bottom: 3px solid #1a73e8 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

col1, col2 = st.columns([1, 6])
with col1:
    st.image("ui/hsbc_google_logo.png", width=120)
with col2:
    st.title("HSBC & Google Cloud RWA Intelligence Platform")
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
# Navigation (State-Driven Tabs)
# ---------------------------------------------------------------------------
TABS = [
    "Upload Policy",
    "Generate SQL",
    "Approve & Execute",
    "SQL Diff",
    "Impact Dashboard",
    "Explainability",
    "Lineage & Audit",
    "Capital Ratios",
    "RWA Analyst",
]

if "active_tab_idx" not in st.session_state:
    st.session_state["active_tab_idx"] = 0

# Visual Tab Bar using columns + buttons
cols = st.columns(len(TABS))
for idx, tab_name in enumerate(TABS):
    if cols[idx].button(
        tab_name, 
        use_container_width=True, 
        type="primary" if st.session_state["active_tab_idx"] == idx else "secondary"
    ):
        st.session_state["active_tab_idx"] = idx
        st.rerun()

active_tab_idx = st.session_state["active_tab_idx"]

# ---------------------------------------------------------------------------
# Tab 1 – Upload Policy PDF
# ---------------------------------------------------------------------------
if active_tab_idx == 0:
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

            st.session_state["demo_policy_id"] = policy_id
            st.session_state["demo_policy_version_id"] = policy_version_id
            
            # Auto-switch to Generate SQL tab
            st.session_state["active_tab_idx"] = 1
            if demo_mode:
                st.session_state["demo_step"] = st.session_state.get("demo_step", 0) + 1
            st.rerun()

# ---------------------------------------------------------------------------
# Tab 2 – Generate SQL (Enhancement 2: streaming progress + B: schema drift)
# ---------------------------------------------------------------------------
if active_tab_idx == 1:
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

            # Auto-switch to Approve & Execute tab
            st.session_state["active_tab_idx"] = 2
            if demo_mode:
                st.session_state["demo_step"] = st.session_state.get("demo_step", 0) + 1
            st.rerun()

    # Enhancement B: Schema Drift Warning
    st.markdown("---")
    st.markdown("#### Schema Drift Check")
    st.caption("Verify a generated SQL version is still compatible with the current BigQuery schema")
    drift_svid = st.text_input(
        "sql_version_id to check",
        value=st.session_state.get("demo_sql_version_id", ""),
        key="drift_svid",
    )
    if drift_svid and st.button("Check Schema Drift", key="check_drift"):
        with st.spinner("Comparing stored schema snapshot to live schema..."):
            drift = service.repo.get_schema_drift(drift_svid)
        added = drift.get("added", [])
        removed = drift.get("removed", [])
        if drift.get("error"):
            st.warning(drift["error"])
        elif not added and not removed:
            st.success("No schema drift detected — SQL is compatible with the current schema.")
        else:
            if added:
                st.warning(
                    f"**{len(added)} new column(s)** added since SQL was generated — "
                    "consider regenerating SQL to incorporate new fields."
                )
                st.code("\n".join(added))
            if removed:
                st.error(
                    f"**{len(removed)} column(s) removed** since SQL was generated — "
                    "SQL execution may fail."
                )
                st.code("\n".join(removed))

# ---------------------------------------------------------------------------
# Tab 3 – Approve + Execute
# ---------------------------------------------------------------------------
if active_tab_idx == 2:
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
            if "demo_run_baseline" not in st.session_state:
                st.session_state["demo_run_baseline"] = run_id
            else:
                st.session_state["demo_run_updated"] = run_id

            if demo_mode:
                st.session_state["demo_step"] = st.session_state.get("demo_step", 0) + 1

# ---------------------------------------------------------------------------
# Tab 4 – SQL Diff Viewer (Enhancement 3)
# ---------------------------------------------------------------------------
if active_tab_idx == 3:
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

        lines_a = sql_a.splitlines()
        lines_b = sql_b.splitlines()
        added = sum(1 for line in difflib.unified_diff(lines_a, lines_b) if line.startswith("+") and not line.startswith("+++"))
        removed = sum(1 for line in difflib.unified_diff(lines_a, lines_b) if line.startswith("-") and not line.startswith("---"))
        st.caption(f"Lines added: **{added}** · Lines removed: **{removed}** · Net: **{added - removed:+d}**")
    elif not diff_pid:
        st.info("Enter a policy_id and click Load to see SQL version diffs.")

# ---------------------------------------------------------------------------
# Tab 5 – RWA Delta Impact Dashboard (Enhancement 4 + D: Stress Test)
# ---------------------------------------------------------------------------
if active_tab_idx == 4:
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

            # Enhancement D: Stress Test Overlay
            st.markdown("---")
            st.markdown("#### Stress Test Overlay")
            stress_mult = st.slider(
                "Stress multiplier",
                min_value=1.0,
                max_value=3.0,
                value=1.0,
                step=0.1,
                format="%.1fx",
                help="Apply a multiplier to updated RWA to simulate stressed market conditions",
                key="stress_mult",
            )
            if stress_mult > 1.0:
                stressed_total = total_updated * stress_mult
                s1, s2, s3 = st.columns(3)
                s1.metric(
                    f"Stressed RWA ({stress_mult:.1f}x)",
                    f"${stressed_total / 1e6:,.2f}M",
                    delta=f"+{(stressed_total - total_updated) / 1e6:,.2f}M vs updated",
                )
                s2.metric("Stress Factor", f"{stress_mult:.1f}x")
                incremental_capital = (stressed_total - total_updated) * 0.08
                s3.metric(
                    "Additional Capital Required",
                    f"${incremental_capital / 1e6:,.2f}M",
                    help="At 8% minimum capital ratio (Basel III Pillar 1)",
                )

                fig_stress = go.Figure()
                fig_stress.add_trace(go.Bar(
                    name="Baseline",
                    x=labels,
                    y=[float(r["rwa_baseline"]) for r in data],
                    marker_color="#6c8ebf",
                ))
                fig_stress.add_trace(go.Bar(
                    name="Updated",
                    x=labels,
                    y=[float(r["rwa_updated"]) for r in data],
                    marker_color="#d4a373",
                ))
                fig_stress.add_trace(go.Bar(
                    name=f"Stressed ({stress_mult:.1f}x)",
                    x=labels,
                    y=[float(r["rwa_updated"]) * stress_mult for r in data],
                    marker_color="#dc3545",
                    opacity=0.75,
                ))
                fig_stress.update_layout(
                    barmode="group",
                    title=f"RWA with {stress_mult:.1f}x Stress Scenario",
                    yaxis_title="RWA Amount",
                    template="plotly_white",
                    height=400,
                )
                st.plotly_chart(fig_stress, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 6 – Clause-to-SQL Explainability (Enhancement 5)
# ---------------------------------------------------------------------------
if active_tab_idx == 5:
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
                st.info("No structured clause citations available.")
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
# Tab 7 – Lineage & Audit (Enhancement 6 + C: Timeline + E: Audit Export)
# ---------------------------------------------------------------------------
if active_tab_idx == 6:
    st.subheader("Lineage & Audit Explorer")
    st.caption("Trace any output back to its source policy")

    # Enhancement C: Policy Version Timeline
    st.markdown("#### Policy Version Timeline")
    timeline_pid = st.text_input(
        "policy_id for timeline",
        value=st.session_state.get("demo_policy_id", ""),
        key="timeline_pid",
    )
    if timeline_pid and st.button("Load Timeline", key="load_timeline"):
        tl_data = service.repo.get_policy_timeline(timeline_pid)
        st.session_state["timeline_data"] = tl_data

    tl_data = st.session_state.get("timeline_data", [])
    if tl_data:
        # Build Plotly timeline
        import plotly.express as px

        events_x, events_y, events_text, events_color = [], [], [], []
        for row in tl_data:
            # Upload event
            if row.get("uploaded_at"):
                events_x.append(row["uploaded_at"])
                events_y.append("Upload")
                events_text.append(f"{row['policy_version_id']}<br>by {row.get('uploaded_by', 'N/A')}")
                events_color.append("#3b82f6")
            # Approval event
            if row.get("approved_at"):
                events_x.append(row["approved_at"])
                events_y.append("SQL Approval")
                events_text.append(f"{row.get('sql_version_id', 'N/A')}<br>by {row.get('approved_by', 'N/A')}")
                events_color.append("#10b981")
            # Run event
            if row.get("started_at"):
                events_x.append(row["started_at"])
                events_y.append("Run")
                events_text.append(f"{row.get('run_id', 'N/A')}<br>{row.get('run_status', 'N/A')}")
                events_color.append("#8b5cf6")

        fig_tl = go.Figure()
        fig_tl.add_trace(go.Scatter(
            x=events_x,
            y=events_y,
            mode="markers+text",
            text=events_text,
            textposition="top center",
            marker=dict(size=14, color=events_color, line=dict(width=2, color="white")),
            hovertext=events_text,
        ))
        fig_tl.update_layout(
            title=f"Policy Lifecycle Timeline — {timeline_pid}",
            xaxis_title="Time",
            yaxis_title="Event Type",
            template="plotly_white",
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_tl, use_container_width=True)

    st.markdown("---")

    # Lineage graph section
    st.markdown("#### Run Lineage Graph")
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

        # Enhancement E: One-Click Audit Package Export
        st.markdown("---")
        st.markdown("#### Export Audit Package")
        st.caption("Download a complete audit-ready zip: lineage metadata, generated SQL, RWA comparison, and lineage graph")

        if st.button("Prepare Audit Package", key="prep_audit"):
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                # 1. Lineage metadata
                zf.writestr("lineage_metadata.json", json.dumps(chain, indent=2, default=str))

                # 2. Generated SQL
                sql_content = service.repo.get_latest_generated_sql(chain.get("sql_version_id", ""))
                if sql_content:
                    zf.writestr("generated_sql.sql", sql_content)

                # 3. RWA comparison CSV
                impact_data = st.session_state.get("impact_data", [])
                if impact_data:
                    csv_buf = io.StringIO()
                    writer = csv.DictWriter(csv_buf, fieldnames=list(impact_data[0].keys()))
                    writer.writeheader()
                    writer.writerows([{k: str(v) for k, v in row.items()} for row in impact_data])
                    zf.writestr("rwa_comparison.csv", csv_buf.getvalue())

                # 4. Mermaid lineage graph source
                mermaid_src = f"""graph LR
    PDF[Policy PDF {pid}] --> VER[Version {pvid}]
    VER --> EXT[Extraction model: {chain.get('model_version', 'N/A')}]
    EXT --> SQL[SQL {svid}]
    SQL --> APPROVE[Approved by {chain.get('approved_by', 'N/A')}]
    APPROVE --> RUN[Run {rid}]
    RUN --> OUT[RWA Outputs]
"""
                zf.writestr("lineage_graph.mermaid", mermaid_src)

                # 5. Run summary
                run_summary = {k: str(v) for k, v in {
                    "run_id": chain.get("run_id"),
                    "policy_id": chain.get("policy_id"),
                    "policy_version_id": chain.get("policy_version_id"),
                    "sql_version_id": chain.get("sql_version_id"),
                    "run_status": chain.get("run_status"),
                    "run_started": chain.get("run_started"),
                    "run_ended": chain.get("run_ended"),
                    "approved_by": chain.get("approved_by"),
                    "approved_at": chain.get("approved_at"),
                    "uploaded_by": chain.get("uploaded_by"),
                    "gcs_uri": chain.get("gcs_uri"),
                }.items()}
                zf.writestr("run_summary.json", json.dumps(run_summary, indent=2))

            buf.seek(0)
            run_id_short = chain.get("run_id", "audit")
            st.download_button(
                label="Download Audit Package (.zip)",
                data=buf,
                file_name=f"audit_package_{run_id_short}.zip",
                mime="application/zip",
                key="download_audit",
            )

    # Raw table browser
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

# ---------------------------------------------------------------------------
# Tab 8 – Capital Adequacy Ratios (Enhancement A)
# ---------------------------------------------------------------------------
if active_tab_idx == 7:
    st.subheader("Capital Adequacy Ratios")
    st.caption("CET1 and Tier 1 ratios computed from live RWA outputs · Basel III thresholds")

    try:
        cap_metrics = service.repo.get_dashboard_metrics()
        live_rwa_m = cap_metrics["total_rwa"] / 1e6 if cap_metrics["total_rwa"] > 0 else 22.0
    except Exception:
        live_rwa_m = 22.0

    cap_col1, cap_col2 = st.columns([1, 2])
    with cap_col1:
        cet1_capital = st.number_input("CET1 Capital ($M)", value=5.0, min_value=0.1, step=0.5, key="cet1_cap")
        tier1_capital = st.number_input("Tier 1 Capital ($M)", value=6.5, min_value=0.1, step=0.5, key="tier1_cap")
        total_capital = st.number_input("Total Capital ($M)", value=8.0, min_value=0.1, step=0.5, key="total_cap")
        st.metric("Total RWA (live)", f"${live_rwa_m:,.2f}M")

        cet1_ratio = (cet1_capital / live_rwa_m * 100) if live_rwa_m > 0 else 0
        tier1_ratio = (tier1_capital / live_rwa_m * 100) if live_rwa_m > 0 else 0
        total_ratio = (total_capital / live_rwa_m * 100) if live_rwa_m > 0 else 0

        st.metric(
            "CET1 Ratio",
            f"{cet1_ratio:.2f}%",
            delta=f"{cet1_ratio - 4.5:+.2f}% vs 4.5% floor",
            delta_color="normal",
        )
        st.metric(
            "Tier 1 Ratio",
            f"{tier1_ratio:.2f}%",
            delta=f"{tier1_ratio - 6.0:+.2f}% vs 6.0% floor",
            delta_color="normal",
        )
        st.metric(
            "Total Capital Ratio",
            f"{total_ratio:.2f}%",
            delta=f"{total_ratio - 8.0:+.2f}% vs 8.0% floor",
            delta_color="normal",
        )

    with cap_col2:
        fig_gauges = go.Figure()

        for i, (label, value, ref, domain_x) in enumerate([
            ("CET1 Ratio (%)", cet1_ratio, 4.5, [0.0, 0.3]),
            ("Tier 1 Ratio (%)", tier1_ratio, 6.0, [0.35, 0.65]),
            ("Total Capital (%)", total_ratio, 8.0, [0.7, 1.0]),
        ]):
            fig_gauges.add_trace(go.Indicator(
                mode="gauge+number+delta",
                value=value,
                title={"text": label, "font": {"size": 13}},
                delta={"reference": ref, "valueformat": ".2f", "suffix": "%"},
                number={"suffix": "%", "valueformat": ".2f"},
                gauge={
                    "axis": {"range": [0, 20], "tickwidth": 1, "tickformat": ".0f"},
                    "bar": {"color": "#0d6efd" if value >= ref else "#dc3545"},
                    "steps": [
                        {"range": [0, ref], "color": "#fee2e2"},
                        {"range": [ref, ref * 1.5], "color": "#fef9c3"},
                        {"range": [ref * 1.5, 20], "color": "#dcfce7"},
                    ],
                    "threshold": {
                        "line": {"color": "#dc3545", "width": 3},
                        "thickness": 0.75,
                        "value": ref,
                    },
                },
                domain={"x": domain_x, "y": [0, 1]},
            ))

        fig_gauges.update_layout(
            height=380,
            template="plotly_white",
            margin=dict(t=60, b=40),
        )
        st.plotly_chart(fig_gauges, use_container_width=True)

        st.caption(
            "🔴 Below minimum · 🟡 Caution (≤1.5× floor) · 🟢 Adequately capitalized  "
            "| Basel III minimums: CET1 4.5% · Tier 1 6.0% · Total 8.0%"
        )

        # Capital buffer analysis
        st.markdown("#### Capital Buffer Analysis")
        buf_col1, buf_col2, buf_col3 = st.columns(3)
        buf_col1.metric(
            "CET1 Buffer",
            f"${(cet1_capital - live_rwa_m * 0.045):,.2f}M",
            help="Capital above 4.5% CET1 minimum",
            delta=f"{cet1_ratio - 4.5:+.2f}pp headroom",
        )
        buf_col2.metric(
            "Tier 1 Buffer",
            f"${(tier1_capital - live_rwa_m * 0.06):,.2f}M",
            help="Capital above 6.0% Tier 1 minimum",
            delta=f"{tier1_ratio - 6.0:+.2f}pp headroom",
        )
        buf_col3.metric(
            "Total Capital Buffer",
            f"${(total_capital - live_rwa_m * 0.08):,.2f}M",
            help="Capital above 8.0% total minimum",
            delta=f"{total_ratio - 8.0:+.2f}pp headroom",
        )

# ---------------------------------------------------------------------------
# Tab 9 – RWA Analyst: Natural Language Query (Enhancement F)
# ---------------------------------------------------------------------------
if active_tab_idx == 8:
    st.subheader("RWA Analyst")
    st.caption("Ask Gemini a natural language question about your RWA data")

    nl_question = st.text_area(
        "Question",
        placeholder="e.g. What drove the increase in Corporate RWA between the two runs?",
        height=100,
        key="nl_question",
    )

    # Build context from available session data
    nl_context: dict = {}
    impact_data = st.session_state.get("impact_data")
    if impact_data:
        nl_context["rwa_comparison"] = impact_data
    chain = st.session_state.get("lineage_chain")
    if chain:
        nl_context["lineage"] = {
            k: v for k, v in chain.items()
            if k in ("policy_id", "policy_version_id", "sql_version_id", "run_id",
                     "run_status", "approved_by", "validation_status")
        }
    try:
        nl_metrics = service.repo.get_dashboard_metrics()
        nl_context["dashboard_metrics"] = nl_metrics
    except Exception:
        pass

    st.caption(
        f"Context loaded: {', '.join(nl_context.keys()) if nl_context else 'none — run a comparison first for richer answers'}"
    )

    if st.button("Ask Gemini", type="primary", disabled=not nl_question.strip()):
        if not nl_context:
            st.warning("No RWA data in session yet. Run a comparison in Tab 5 first for grounded answers.")
        else:
            with st.spinner("Gemini is analysing your RWA data..."):
                answer = service.agent.answer_rwa_question(nl_question, nl_context)
            st.markdown("#### Answer")
            st.markdown(answer)

    # Suggested questions
    st.markdown("---")
    st.markdown("**Suggested questions:**")
    suggestions = [
        "What drove the increase in Corporate RWA between the two runs?",
        "Which portfolio has the highest risk-weighted assets and why?",
        "How does the policy change impact our capital adequacy position?",
        "What is the net RWA impact of moving from baseline to updated policy?",
        "Which risk bucket shows the most sensitivity to the policy update?",
    ]
    for s in suggestions:
        st.markdown(f"- *{s}*")
