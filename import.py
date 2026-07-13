"""
Databricks -> SqlDBM DDL importer  (single-cell interactive app).

Run from a Databricks notebook cell:

    import urllib.request
    url = "https://raw.githubusercontent.com/sqldbmcorp/poc-databricks-re-notebook/refs/heads/main/import.py"
    exec(compile(urllib.request.urlopen(url).read().decode(), "import.py", "exec"), globals())

Renders one form: pick a source catalog + schema(s), list objects, select them, generate DDL, then configure the
SqlDBM destination (token, project/branch/revision), review, and Submit.

Requirements: ipywidgets (current DBR / serverless), Unity Catalog read access, and outbound
HTTPS to api.sqldbm.com. `spark` is taken from the calling notebook's globals.
"""

import os, json, time, html, requests, functools
from datetime import datetime
import ipywidgets as widgets
from IPython.display import display, HTML

# spark comes from the calling notebook; fall back to a session if exec'd elsewhere
try:
    spark  # noqa: F821
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

# ============================================================ config
SQLDBM_BASE = "https://api.sqldbm.com"   # production
DB_TYPES = ["databricks", "snowflake", "sqlServer", "postgreSQL", "redshift",
            "azureSynapse", "bigQuery", "oracle", "mySQL", "alloyDB", "logical"]
URL_DBTYPE = {"databricks": "Databricks", "snowflake": "Snowflake", "sqlServer": "SQLServer",
              "postgreSQL": "PostgreSQL", "redshift": "Redshift", "azureSynapse": "AzureSynapse",
              "bigQuery": "BigQuery", "oracle": "Oracle", "mySQL": "MySQL",
              "alloyDB": "AlloyDB", "logical": "Logical"}
# A branch is addressed by its own id in the same p<id> slot as a project.
BRANCH_URL_TEMPLATE = "https://app.sqldbm.com/{seg}/DatabaseExplorer/p{branch_id}/"
# Where this script is hosted (used by the preflight reachability check).
RAW_URL = "https://raw.githubusercontent.com/sqldbmcorp/poc-databricks-re-notebook/refs/heads/main/import.py"

# ============================================================ SqlDBM API client
def _headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _get(path, token):
    r = requests.get(f"{SQLDBM_BASE}{path}", headers=_headers(token), timeout=60)
    r.raise_for_status()
    return r.json()

def list_projects(token):
    return _get("/projects", token).get("data", []) or []

def list_branches(token, project_id):
    try:
        return (_get(f"/projects/{project_id}/branches", token).get("data", {}) or {}).get("branches", []) or []
    except requests.HTTPError:
        return []

def list_revisions(token, project_id, branch_id=None):
    base = f"/projects/{project_id}" + (f"/branches/{branch_id}" if branch_id else "")
    try:
        data = _get(f"{base}/revisions", token).get("data", [])
    except requests.HTTPError:
        return []
    if isinstance(data, dict):
        data = data.get("revisions", []) or []
    return data or []

def _diagram_block(diagram_name):
    return [{"subjectArea": None, "diagramName": diagram_name}] if diagram_name else None

def create_project(token, project_name, ddl, db_type, revision_name, diagram_name=None):
    body = {"dbType": db_type, "projectName": project_name, "sourceFormat": "ddl",
            "payload": ddl, "revisionName": revision_name}
    dg = _diagram_block(diagram_name)
    if dg:
        body["addToDiagram"] = dg
    return requests.post(f"{SQLDBM_BASE}/projects", headers=_headers(token),
                         data=json.dumps(body), timeout=120)

def create_revision(token, project_id, ddl, revision_name, strict=False,
                    branch_id=None, revision_id=None, diagram_name=None):
    base = f"/projects/{project_id}" + (f"/branches/{branch_id}" if branch_id else "")
    target = f"/revisions/{revision_id}" if revision_id else "/revisions/last"
    body = {"sourceFormat": "ddl", "payload": ddl, "strictMode": strict, "revisionName": revision_name}
    dg = _diagram_block(diagram_name)
    if dg:
        body["addToDiagram"] = dg
    return requests.post(f"{SQLDBM_BASE}{base}{target}", headers=_headers(token),
                         data=json.dumps(body), timeout=120)

def create_branch(token, project_id, branch_name, revision_name="Initial branch revision"):
    return requests.post(f"{SQLDBM_BASE}/projects/{project_id}/branches", headers=_headers(token),
                         data=json.dumps({"branchName": branch_name, "revisionName": revision_name}),
                         timeout=120)

def wait_for_branch(token, project_id, branch_name, tries=10, delay=1.5):
    for _ in range(tries):
        for b in list_branches(token, project_id):
            if b.get("branchName") == branch_name:
                return b.get("branchId")
        time.sleep(delay)
    return None

def wait_for_project(token, project_name, tries=10, delay=1.5):
    target = project_name.strip().lower()
    for _ in range(tries):
        for p in list_projects(token):
            if str(p.get("name", "")).strip().lower() == target:
                return p.get("id")
        time.sleep(delay)
    return None

def get_project_dbtype(token, project_id):
    try:
        info = (_get(f"/projects/{project_id}/revisions/last", token).get("data", {}) or {}).get("projectInfo", {}) or {}
        return info.get("dbType")
    except Exception:
        return None

def project_link(db_type_segment, project_id):
    return f"https://app.sqldbm.com/{db_type_segment}/DatabaseExplorer/p{project_id}/"

def branch_link(db_type_segment, project_id, branch_id):
    return BRANCH_URL_TEMPLATE.format(seg=db_type_segment, branch_id=branch_id)

# current-user + timestamp for default new-branch name
try:
    _user = spark.sql("SELECT current_user() AS u").first()["u"]
except Exception:
    import getpass
    _user = getpass.getuser()
_user_slug = (_user or "user").split("@")[0].replace(".", "-")
RUN_TS = datetime.now().strftime("%Y%m%d-%H%M%S")
DEFAULT_BRANCH_NAME = f"databricks-import/{_user_slug}-{RUN_TS}"

# ============================================================ shared state (source side)
results = []            # every object listed from the chosen schemas (ddl populated in step 2)
selected = {}           # object_key -> bool  (selection lives here, NOT in widgets, so it scales)
_schema_headers = {}    # schema -> (widgets.HTML, objs_list) — live header widgets for dynamic counts
catalog = ""
selected_schemas = []
RENDER_CAP = 300        # max checkbox rows rendered at once; filter to reach the rest

NEW_PROJECT = "➕  Create new project"
NEW_BRANCH = "➕  Create new branch"
SELECT_BRANCH = "— select a branch —"
LATEST = "Create revision on latest"
_state = {"projects": {}, "branch_ids": {}, "revision_ids": {}}
_W = {"width": "420px"}
_S = {"description_width": "120px"}

# ============================================================ SOURCE controls
def _list_catalogs():
    return sorted(r[0] for r in spark.sql("SHOW CATALOGS").collect())

def _list_schemas(cat):
    # Mirrors the tool's DatabricksGetStructure: set current catalog, then listDatabases().
    spark.catalog.setCurrentCatalog(cat)
    return sorted(d.name for d in spark.catalog.listDatabases())

catalog_dd   = widgets.Dropdown(description="Catalog", options=[],
                                layout=widgets.Layout(**_W), style=_S)
schema_sel   = widgets.SelectMultiple(description="Schema(s)", options=[], rows=8,
                                      layout=widgets.Layout(**_W), style=_S)
generate_btn = widgets.Button(description="List Objects", button_style="primary",
                              layout=widgets.Layout(width="220px", margin="10px 130px"))
source_out   = widgets.Output()

# ============================================================ STEP 2 controls (object picker)
filter_w         = widgets.Text(description="Filter", placeholder="name contains… (e.g. fact_, dim_, schema.table)",
                                continuous_update=False, layout=widgets.Layout(width="340px"), style=_S)
filter_btn       = widgets.Button(description="Apply", layout=widgets.Layout(width="80px"))
objects_summary  = widgets.HTML("List objects in Step 1 to populate this list.")
objects_container= widgets.VBox([], layout=widgets.Layout(margin="0 0 0 128px"))
options_label    = widgets.Label("Options", layout=widgets.Layout(width="120px", display="flex",
                                                                  justify_content="flex-end"))
select_all_btn   = widgets.Button(description="Select all", layout=widgets.Layout(width="140px"))
select_none_btn  = widgets.Button(description="Deselect all", layout=widgets.Layout(width="140px"))
step2_back_btn   = widgets.Button(description="◂  Back", layout=widgets.Layout(width="100px"))
continue_btn     = widgets.Button(description="Generate DDL for Selected ▸", button_style="primary",
                                  layout=widgets.Layout(width="260px"))
continue_status  = widgets.HTML("")
# ---- Step 3 (DDL confirmation) ----
preview_out      = widgets.Output()
back_btn         = widgets.Button(description="◂  Back", layout=widgets.Layout(width="100px"))
confirm_btn      = widgets.Button(description="Confirm & Configure Destination ▸", button_style="success",
                                  layout=widgets.Layout(width="280px"))

def on_catalog_change(_=None):
    schema_sel.options = ["⏳ Loading schemas…"]
    schema_sel.disabled = True
    try:
        schema_sel.options = _list_schemas(catalog_dd.value)
    except Exception as e:
        schema_sel.options = []
        with source_out:
            print(f"Could not list schemas for '{catalog_dd.value}': {e}")
    finally:
        schema_sel.disabled = False

def _kind_from_tabletype(table_type):
    tt = (table_type or "").upper()
    if tt == "VIEW":
        return "VIEW"
    if tt == "STREAMING_TABLE":
        return "STREAMING TABLE"
    if tt == "MATERIALIZED_VIEW":
        return "MATERIALIZED VIEW"
    return "TABLE"

def _list_user_functions(cat, schema):
    """Return bare function names for user-defined functions in catalog.schema."""
    try:
        rows = spark.sql(f"SHOW USER FUNCTIONS IN `{cat}`.`{schema}`").collect()
        return [row[0].rsplit(".", 1)[-1] for row in rows]
    except Exception:
        try:
            rows = spark.sql(f"SHOW USER FUNCTIONS IN `{schema}`").collect()
            return [row[0].rsplit(".", 1)[-1] for row in rows]
        except Exception:
            return []

def _show_create(r):
    """Run the appropriate SHOW CREATE statement for the object."""
    cat, sch, name, kind = r["catalog"], r["schema"], r["name"], r["kind"]
    if kind == "FUNCTION":
        try:
            return spark.sql(f"SHOW CREATE FUNCTION `{cat}`.`{sch}`.`{name}`").first()[0]
        except Exception:
            return spark.sql(f"SHOW CREATE FUNCTION `{sch}`.`{name}`").first()[0]
    else:
        return spark.sql(f"SHOW CREATE TABLE `{sch}`.`{name}`").first()[0]

def on_list_objects(_):
    global results, catalog, selected_schemas
    source_out.clear_output()
    generate_btn.disabled = True
    generate_btn.description = "Listing…"
    with source_out:
        display(HTML("<div style='font-size:13px;color:#555'>⏳ Listing objects…</div>"))
    catalog = catalog_dd.value
    selected_schemas = list(schema_sel.value)
    source_out.clear_output()
    with source_out:
        if not selected_schemas:
            generate_btn.disabled = False
            generate_btn.description = "List Objects"
            print("Select at least one schema, then click List Objects.")
            return
        res, skipped = [], []
        # Mirrors the tool's DatabricksGetStructure: set current catalog, then
        # listTables(database), skip temporary tables, and list user functions.
        try:
            spark.catalog.setCurrentCatalog(catalog)
        except Exception as e:
            generate_btn.disabled = False
            generate_btn.description = "List Objects"
            print(f"Could not set current catalog '{catalog}': {e}")
            return
        for schema in selected_schemas:
            try:
                tbls = spark.catalog.listTables(schema)
            except Exception as e:
                skipped.append((schema, str(e).splitlines()[0]))
                continue
            for t in tbls:
                if t.isTemporary:
                    continue
                res.append({"catalog": catalog, "schema": schema, "name": t.name,
                            "kind": _kind_from_tabletype(getattr(t, "tableType", None)),
                            "ddl": None})
            for fn in _list_user_functions(catalog, schema):
                res.append({"catalog": catalog, "schema": schema, "name": fn,
                            "kind": "FUNCTION", "ddl": None})
        results = res
        print(f"Found {len(res)} object(s) across {len(selected_schemas)} schema(s). "
              f"{len(skipped)} schema(s) failed. Select objects in Step 2, then generate DDL.")
        for s, reason in skipped[:25]:
            print(f"  - skipped {s}: {reason[:120]}")
    generate_btn.disabled = False
    generate_btn.description = "List Objects"
    global selected
    selected = {_key(r): True for r in results}
    filter_w.value = ""
    render_objects()
    update_counts()
    if results:
        open_step(1)

def _kind(r):
    return r.get("kind") or "TABLE"

# ---- selection helpers (operate on the `selected` dict, never on widget state) ----
def _key(r):
    return f"{r['schema']}.{r['name']}"

def current_matches():
    """Objects matching the filter box (case-insensitive substring on schema.table · kind)."""
    q = filter_w.value.strip().lower()
    if not q:
        return results
    return [r for r in results if q in f"{_key(r)} {_kind(r)}".lower()]

def selected_count():
    return sum(1 for r in results if selected.get(_key(r), False))

def build_payload():
    return "\n".join(f"-- {r['catalog']}.{r['schema']}.{r['name']}\n{r['ddl']};\n"
                     for r in results if selected.get(_key(r), False) and r.get("ddl"))

def on_toggle(change, key):
    selected[key] = change["new"]
    update_counts()

def update_counts(*_):
    """Cheap: refresh counters + the destination review. Does NOT build the DDL payload."""
    total = len(results)
    objects_summary.value = (f"<b>{selected_count()}</b> of {total} object(s) selected"
                             if total else "List objects in Step 1 to populate this list.")
    for schema, (hdr, objs) in _schema_headers.items():
        sel_n = sum(1 for r in objs if selected.get(_key(r), False))
        hdr.value = (f"<div style='font-weight:600;margin:8px 0 2px'>"
                     f"{html.escape(catalog)}.{html.escape(schema)} "
                     f"<span style='font-weight:400;color:#777'>({sel_n}/{len(objs)} selected)</span></div>")
    render_review()

def render_objects(*_):
    """Render only the filtered slice, capped at RENDER_CAP rows, grouped by schema."""
    global _schema_headers
    if not results:
        objects_container.children = []
        _schema_headers = {}
        return
    matches = current_matches()
    by_schema = {}
    for r in matches:
        by_schema.setdefault(r["schema"], []).append(r)
    _schema_headers = {}
    children, shown, capped = [], 0, False
    for schema in sorted(by_schema):
        objs = sorted(by_schema[schema], key=lambda x: x["name"].lower())
        sel_n = sum(1 for r in objs if selected.get(_key(r), False))
        hdr = widgets.HTML(
            f"<div style='font-weight:600;margin:8px 0 2px'>{html.escape(catalog)}.{html.escape(schema)} "
            f"<span style='font-weight:400;color:#777'>({sel_n}/{len(objs)} selected)</span></div>")
        _schema_headers[schema] = (hdr, objs)
        children.append(hdr)
        for r in objs:
            if shown >= RENDER_CAP:
                capped = True
                break
            k = _key(r)
            chk = widgets.Checkbox(value=selected.get(k, True), indent=False,
                                   description=f"{r['name']}   ·   {_kind(r)}",
                                   layout=widgets.Layout(width="380px", margin="0"))
            chk.observe(functools.partial(on_toggle, key=k), names="value")
            row_widgets = [chk]
            if r.get("ddl"):
                details = widgets.HTML(
                    "<details style='margin:0 0 4px 26px'>"
                    "<summary style='cursor:pointer;font-size:12px;color:#555'>show DDL</summary>"
                    "<div style='max-height:300px;overflow:auto;border:1px solid #ddd;padding:6px;"
                    "font-family:monospace;white-space:pre;font-size:12px;margin-top:4px'>"
                    f"{html.escape(r['ddl'])}</div></details>")
                row_widgets.append(details)
            children.append(widgets.VBox(row_widgets, layout=widgets.Layout(margin="0")))
            shown += 1
        if capped:
            break
    if capped:
        children.append(widgets.HTML(
            f"<div style='color:#a60;margin-top:6px'>Showing first {RENDER_CAP} of {len(matches)} "
            f"matching objects — refine the Filter to see more. Select / Deselect all still apply "
            f"to all {len(matches)} matches.</div>"))
    objects_container.children = children

def set_all_filtered(value):
    """Apply to every object matching the current filter (not just the rendered slice)."""
    for r in current_matches():
        selected[_key(r)] = value
    render_objects()
    update_counts()

def on_preview_continue(_):
    """Step 2 -> Step 3: generate DDL for any newly selected objects, then show the confirmation panel."""
    continue_status.value = ""
    if selected_count() == 0:
        continue_status.value = "<span style='color:#c00'>Select at least one object to continue.</span>"
        return
    to_generate = [r for r in results if selected.get(_key(r), False) and r["ddl"] is None]
    if to_generate:
        continue_btn.disabled = True
        continue_btn.description = "Generating DDL…"
        skipped = []
        for i, r in enumerate(to_generate, 1):
            continue_status.value = f"<span style='color:#555'>⏳ Generating DDL… {i}/{len(to_generate)}</span>"
            try:
                r["ddl"] = _show_create(r)
            except Exception as e:
                skipped.append((_key(r), str(e).splitlines()[0]))
        continue_btn.disabled = False
        continue_btn.description = "Generate DDL for Selected ▸"
        if skipped:
            continue_status.value = (f"<span style='color:#a60'>⚠ {len(skipped)} object(s) failed DDL "
                                     f"generation and will be excluded from the payload.</span>")
            with source_out:
                for k, reason in skipped[:25]:
                    print(f"  - DDL generation failed for {k}: {reason[:120]}")
        else:
            continue_status.value = ""
        render_objects()   # reveal "show DDL" expanders for newly generated objects
    preview_out.clear_output()
    with preview_out:
        payload = build_payload()
        if not payload:
            display(HTML("<div style='color:#c00'>No DDL was successfully generated for the selected objects.</div>"))
            return
        n = sum(1 for r in results if selected.get(_key(r), False) and r.get("ddl"))
        display(HTML(
            f"<div style='font-size:12px;color:#555;margin-bottom:4px'>"
            f"DDL for <b>{n}</b> selected object(s) — review, then confirm:</div>"
            "<div style='max-height:480px;overflow:auto;border:1px solid #ccc;padding:8px;"
            "font-family:monospace;white-space:pre;font-size:12px'>"
            f"{html.escape(payload)}</div>"))
    open_step(2)

def on_confirm(_):
    """Step 3 -> Step 4: open the destination panel."""
    open_step(3)

# ============================================================ DESTINATION controls
token_w        = widgets.Password(description="API Token", layout=widgets.Layout(**_W), style=_S)
connect_btn    = widgets.Button(description="Connect & load projects", button_style="info",
                                layout=widgets.Layout(width="220px"))
project_dd     = widgets.Dropdown(options=[], description="Project", disabled=True,
                                  layout=widgets.Layout(**_W), style=_S)
new_proj_name  = widgets.Text(description="New name", placeholder="Unique project name",
                              layout=widgets.Layout(**_W), style=_S)
cw_chk         = widgets.Checkbox(value=False, indent=False,
                                  description="Concurrent-Working project (route via a branch)",
                                  layout=widgets.Layout(margin="0px 130px"))
branch_dd      = widgets.Dropdown(options=[], description="Branch",
                                  layout=widgets.Layout(**_W), style=_S)
new_branch_w   = widgets.Text(value=DEFAULT_BRANCH_NAME, description="Branch name",
                              layout=widgets.Layout(**_W), style=_S)
update_dd      = widgets.Dropdown(options=[LATEST], value=LATEST, description="Update",
                                  layout=widgets.Layout(**_W), style=_S)
revision_name_w= widgets.Text(value=f"Databricks import {RUN_TS}", description="Revision name",
                              layout=widgets.Layout(**_W), style=_S)
diagram_w      = widgets.Text(value="Reverse engineer", description="Diagram",
                              placeholder="(optional) place objects on a diagram",
                              layout=widgets.Layout(**_W), style=_S)
strict_w       = widgets.Checkbox(value=False, indent=False,
                                  description="strictMode (override another user's lock)",
                                  layout=widgets.Layout(margin="0px 130px"))
submit_btn     = widgets.Button(description="Submit to SqlDBM", button_style="success",
                                disabled=True, layout=widgets.Layout(width="220px"))
status_out     = widgets.Output()
review_out     = widgets.Output()
result_out     = widgets.Output()

def _set(w, show):
    w.layout.display = "" if show else "none"

def name_conflict():
    if project_dd.value == NEW_PROJECT:
        nm = new_proj_name.value.strip().lower()
        if nm and nm in _state.get("project_names", set()):
            return f"Project name '{new_proj_name.value.strip()}' already exists — names must be unique."
    elif project_dd.value and cw_chk.value and branch_dd.value == NEW_BRANCH:
        bn = new_branch_w.value.strip().lower()
        if bn and bn in _state.get("branch_names", set()):
            return f"Branch name '{new_branch_w.value.strip()}' already exists on this project — choose a unique name."
    return ""

def render_review(*_):
    review_out.clear_output()
    with review_out:
        n = selected_count()
        total = len(results)
        dest = "(choose a project)"
        if project_dd.value == NEW_PROJECT:
            dest = f"NEW project '{new_proj_name.value or '...'}' (dbType=databricks)"
        elif project_dd.value:
            dest = f"{project_dd.value}"
            if cw_chk.value:
                if branch_dd.value == SELECT_BRANCH:
                    dest += "  ·  branch: (none selected)"
                elif branch_dd.value == NEW_BRANCH:
                    dest += f"  ·  branch: {new_branch_w.value}"
                else:
                    dest += f"  ·  branch: {branch_dd.value}"
            dest += f"  ·  {update_dd.value}"
        warn = name_conflict()
        warn_html = f"<div style='color:#c00;margin-top:4px'>⚠ {html.escape(warn)}</div>" if warn else ""
        src = f"{html.escape(catalog)} · schema(s): {html.escape(', '.join(selected_schemas))}" if catalog else "(list objects first)"
        display(HTML(
            "<div style='font-family:sans-serif;font-size:13px'>"
            f"<b>Source:</b> {src} · <b>{n}</b> of {total} object(s) selected<br>"
            f"<b>Destination:</b> {html.escape(dest)}<br>"
            f"<b>Revision name:</b> {html.escape(revision_name_w.value)}"
            f"{' · strictMode' if strict_w.value else ''}"
            f"{(' · diagram: ' + html.escape(diagram_w.value)) if diagram_w.value.strip() else ''}"
            f"{warn_html}"
            "<div style='margin-top:6px;color:#777;font-size:12px'>Use “DDL Confirmation” in Step 3 to see the exact payload.</div>"
            "</div>"))
    needs_branch = cw_chk.value and branch_dd.value == SELECT_BRANCH
    submit_btn.disabled = (not bool(project_dd.value)) or bool(name_conflict()) or (selected_count() == 0) or needs_branch

def refresh_conditional_fields():
    is_new = project_dd.value == NEW_PROJECT
    _set(new_proj_name, is_new)
    _set(cw_chk, not is_new and bool(project_dd.value))
    show_branch = (not is_new) and cw_chk.value
    _set(branch_dd, show_branch)
    _set(new_branch_w, show_branch and branch_dd.value == NEW_BRANCH)
    branch_chosen = not cw_chk.value or branch_dd.value not in (SELECT_BRANCH, NEW_BRANCH)
    _set(update_dd, (not is_new) and bool(project_dd.value) and branch_chosen)

def load_project_context(project_id):
    token = token_w.value.strip()
    branches = list_branches(token, project_id)
    is_cw = len(branches) > 1 or any(not b.get("isMain", True) for b in branches)
    cw_chk.value = is_cw
    _state["branch_ids"] = {}
    _state["branch_names"] = {str(b.get("branchName", "")).strip().lower() for b in branches}
    labels = []
    for b in sorted(branches, key=lambda x: str(x.get("branchName", "")).lower()):
        if b.get("isMain"):
            continue
        label = b.get("branchName", "?")
        _state["branch_ids"][label] = b.get("branchId")
        labels.append(label)
    branch_dd.options = [SELECT_BRANCH, NEW_BRANCH] + labels
    branch_dd.value = SELECT_BRANCH
    _state["revision_ids"] = {}
    update_dd.options = [LATEST]
    update_dd.value = LATEST

def on_connect(_):
    status_out.clear_output()
    with status_out:
        token = token_w.value.strip()
        if not token:
            print("Enter your API token first.")
            return
        try:
            projects = list_projects(token)
        except Exception as e:
            print(f"Could not load projects: {e}")
            return
        projects_sorted = sorted(projects, key=lambda p: str(p.get("name", "")).lower())
        _state["projects"] = {f"{p['name']}  (#{p['id']})": p["id"] for p in projects_sorted}
        _state["project_names"] = {str(p.get("name", "")).strip().lower() for p in projects}
        project_dd.options = [NEW_PROJECT] + list(_state["projects"].keys())
        project_dd.value = NEW_PROJECT
        project_dd.disabled = False
        print(f"Connected. {len(projects)} existing project(s) loaded.")
    refresh_conditional_fields()
    render_review()

def on_project_change(_):
    if project_dd.value and project_dd.value != NEW_PROJECT:
        with status_out:
            try:
                load_project_context(_state["projects"][project_dd.value])
            except Exception as e:
                print(f"Could not load project context: {e}")
    refresh_conditional_fields()
    render_review()

def on_submit(_):
    result_out.clear_output()
    with result_out:
        token = token_w.value.strip()
        if not token:
            print("Enter your API token."); return
        if selected_count() == 0:
            print("No objects selected — list objects and select them in Step 2 first."); return
        payload = build_payload()
        conflict = name_conflict()
        if conflict:
            print(f"⛔ {conflict}"); return
        rev_name = revision_name_w.value.strip() or f"Databricks import {RUN_TS}"
        diagram = diagram_w.value.strip() or None
        is_new = project_dd.value == NEW_PROJECT
        created_name = None
        pid = None
        try:
            if is_new:
                created_name = new_proj_name.value.strip()
                if not created_name:
                    print("Enter a name for the new project."); return
                print(f"Creating new project '{created_name}' ...")
                r = create_project(token, created_name, payload, "databricks", rev_name, diagram)
            else:
                pid = _state["projects"][project_dd.value]
                branch_id = None
                branch_name = None
                if cw_chk.value:
                    if branch_dd.value == NEW_BRANCH:
                        branch_name = new_branch_w.value.strip() or DEFAULT_BRANCH_NAME
                        print(f"Creating branch '{branch_name}' ...")
                        cr = create_branch(token, pid, branch_name, rev_name)
                        if cr.status_code not in (200, 202):
                            print(f"❌ Branch create failed {cr.status_code}: {cr.text}"); return
                        branch_id = wait_for_branch(token, pid, branch_name)
                        if not branch_id:
                            print("Branch was accepted but isn't visible yet. Re-select it in a moment and submit."); return
                    else:
                        branch_id = _state["branch_ids"].get(branch_dd.value)
                        branch_name = branch_dd.value.replace("  (main)", "")
                rev_id = None if update_dd.value == LATEST else _state["revision_ids"].get(update_dd.value)
                where = f"branch {branch_id}" if branch_id else "main"
                tgt = f"revision {rev_id}" if rev_id else "latest"
                print(f"Creating revision on {where} ({tgt}) ...")
                r = create_revision(token, pid, payload, rev_name, strict_w.value, branch_id, rev_id, diagram)

            if r.status_code in (200, 202):
                print(f"✅ {r.status_code} — accepted. SqlDBM is processing the import.")
                try:
                    if is_new:
                        new_id = wait_for_project(token, created_name)
                        if new_id:
                            seg = get_project_dbtype(token, new_id) or URL_DBTYPE.get("databricks", "databricks")
                            _show_links(seg, new_id)
                        else:
                            print("New project accepted but not queryable yet — open SqlDBM to find it shortly.")
                    else:
                        seg = get_project_dbtype(token, pid)
                        if seg:
                            _show_links(seg, pid, branch_id, branch_name)
                        else:
                            print("Submitted OK; open SqlDBM to view the project "
                                  "(couldn't resolve dbType to build a link).")
                except Exception as e:
                    print(f"(submitted OK; couldn't build link: {e})")
            else:
                print(f"❌ {r.status_code}: {r.text}")
        except Exception as e:
            print(f"Error: {e}")

def refresh_revisions_for_branch(_=None):
    if not project_dd.value or project_dd.value == NEW_PROJECT:
        return
    if branch_dd.value in (NEW_BRANCH, SELECT_BRANCH):
        return
    token = token_w.value.strip()
    if not token:
        return
    project_id = _state["projects"].get(project_dd.value)
    if not project_id:
        return
    branch_id = _state["branch_ids"].get(branch_dd.value)
    _state["revision_ids"] = {}
    revs = []
    for rv in list_revisions(token, project_id, branch_id):
        rid = rv.get("revisionId") or rv.get("id")
        if rid is None:
            continue
        num = rv.get("revNumber") or rv.get("number") or rid
        nm = rv.get("revName") or rv.get("name") or ""
        revs.append((num, rid, nm))
    rev_opts = [LATEST]
    for num, rid, nm in sorted(revs, key=lambda t: (t[0] if isinstance(t[0], int) else 0), reverse=True):
        label = f"From revision {num}: {nm}".strip()
        _state["revision_ids"][label] = rid
        rev_opts.append(label)
    update_dd.options = rev_opts
    update_dd.value = LATEST

def _link_html(label, url):
    return (f"<div style='margin-top:6px'>🔗 <b>{html.escape(label)}:</b> "
            f"<a href='{html.escape(url)}' target='_blank'>{html.escape(url)}</a></div>")

def _show_links(seg, project_id, branch_id=None, branch_name=None):
    display(HTML(_link_html("Main branch", project_link(seg, project_id))))
    if branch_id is not None:
        label = f"Branch '{branch_name}'" if branch_name else "Branch"
        display(HTML(_link_html(label, branch_link(seg, project_id, branch_id))))

# ============================================================ wire up
catalog_dd.observe(on_catalog_change, names="value")
generate_btn.on_click(on_list_objects)
filter_w.observe(render_objects, names="value")
filter_btn.on_click(lambda _: render_objects())
select_all_btn.on_click(lambda b: set_all_filtered(True))
select_none_btn.on_click(lambda b: set_all_filtered(False))
step2_back_btn.on_click(lambda _: open_step(0))
continue_btn.on_click(on_preview_continue)
back_btn.on_click(lambda _: open_step(1))
confirm_btn.on_click(on_confirm)
connect_btn.on_click(on_connect)
submit_btn.on_click(on_submit)
project_dd.observe(on_project_change, names="value")
cw_chk.observe(lambda c: (refresh_conditional_fields(), render_review()), names="value")
branch_dd.observe(lambda c: (refresh_conditional_fields(), refresh_revisions_for_branch(), render_review()), names="value")
for _w in (new_proj_name, update_dd, revision_name_w, diagram_w, strict_w, new_branch_w):
    _w.observe(render_review, names="value")

# ============================================================ initialize + render
catalog_dd.options = _list_catalogs()
try:
    _cur = spark.catalog.currentCatalog()
    if _cur in catalog_dd.options:
        catalog_dd.value = _cur
except Exception:
    pass
on_catalog_change()
refresh_conditional_fields()
render_review()

STEP_TITLES = [
    "1 · Select Catalog and Schema(s)",
    "2 · Select Objects and Generate DDL",
    "3 · DDL Confirmation",
    "4 · Configure Destination Project",
]
_step1 = widgets.VBox([catalog_dd, schema_sel, generate_btn, source_out])
_step2 = widgets.VBox([
    widgets.HBox([step2_back_btn, continue_btn, continue_status]),
    widgets.HBox([filter_w, filter_btn]),
    widgets.HBox([options_label, select_all_btn, select_none_btn, objects_summary]),
    objects_container,
])
_step3 = widgets.VBox([
    widgets.HBox([back_btn, confirm_btn]),
    preview_out,
])
_step4 = widgets.VBox([
    widgets.HBox([token_w, connect_btn]), status_out,
    project_dd, new_proj_name,
    cw_chk, branch_dd, new_branch_w,
    update_dd, revision_name_w, diagram_w, strict_w,
    widgets.HTML("<hr style='margin:8px 0'>"),
    review_out, submit_btn, result_out,
])
step_contents = [_step1, _step2, _step3, _step4]

# Header buttons double as the (always-visible) section titles.
step_headers = [widgets.Button(description=t, layout=widgets.Layout(display="flex", width="100%", margin="2px 0", align_items="flex-start"))
                for t in STEP_TITLES]
_open = {"i": 0}

def open_step(i):
    """Open panel i (collapsing the rest); pass -1 to collapse all. Headers stay visible either way."""
    _open["i"] = i
    for j, (h, c) in enumerate(zip(step_headers, step_contents)):
        is_open = (j == i)
        c.layout.display = "" if is_open else "none"
        h.description = ("▾  " if is_open else "▸  ") + STEP_TITLES[j]
        h.style.button_color = "#dbe6ff" if is_open else "#f2f2f2"

def _header_handler(i):
    def _h(_):
        open_step(-1 if _open["i"] == i else i)   # click the open one to collapse it
    return _h

for _i, _h in enumerate(step_headers):
    _h.on_click(_header_handler(_i))

open_step(0)   # start on Step 1; the rest collapse but their titles remain visible

# ============================================================ environment preflight
preflight_btn = widgets.Button(description="Re-run environment checks",
                               layout=widgets.Layout(width="240px"))
preflight_out = widgets.Output()

def _probe(url, timeout=5):
    """Return (reachable, status_code|None, detail). reachable=True if any HTTP response came back."""
    try:
        r = requests.get(url, timeout=timeout)
        return True, r.status_code, f"HTTP {r.status_code}"
    except requests.exceptions.SSLError:
        return False, None, "TLS/SSL error — likely an inspecting proxy or certificate issue"
    except requests.exceptions.RequestException as e:
        return False, None, f"unreachable ({type(e).__name__})"

def _runtime_info():
    try:
        dbr = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    except Exception:
        dbr = os.environ.get("DATABRICKS_RUNTIME_VERSION") or "unknown"
    compute = "Serverless / Spark Connect" if "connect" in type(spark).__module__ else "Classic cluster"
    return dbr, compute

def _catalog_info():
    try:
        cats = sorted(r[0] for r in spark.sql("SHOW CATALOGS").collect())
    except Exception as e:
        return "?", [], False, f"SHOW CATALOGS failed: {str(e).splitlines()[0][:80]}"
    try:
        cur = spark.catalog.currentCatalog()
    except Exception:
        cur = "?"
    uc = ("system" in cats) or ("hive_metastore" in cats) or \
         any(c not in {"spark_catalog", "samples", "hive_metastore"} for c in cats)
    note = "Unity Catalog appears ENABLED" if uc else "No Unity Catalog detected (Hive metastore only)"
    return cur, cats, uc, note

def run_preflight(_=None):
    preflight_out.clear_output()
    with preflight_out:
        dbr, compute = _runtime_info()
        cur, cats, uc, uc_note = _catalog_info()
        sql_reach, sql_code, _sql_d = _probe(SQLDBM_BASE + "/swagger/v1/swagger.json")
        gh_reach, gh_code, gh_d = _probe(RAW_URL)

        marks = {"ok": ("✅", "#137333"), "warn": ("⚠️", "#a60"),
                 "bad": ("❌", "#c00"), "info": ("ℹ️", "#333")}

        def line(state, label, detail, tip=""):
            icon, color = marks[state]
            if tip:
                label_html = (f"<span title=\"{html.escape(tip)}\" style='cursor:help;"
                              f"border-bottom:1px dotted #999'><b>{html.escape(label)}</b></span>")
            else:
                label_html = f"<b>{html.escape(label)}</b>"
            return (f"<tr><td style='padding:2px 8px'>{icon}</td>"
                    f"<td style='padding:2px 8px'>{label_html}</td>"
                    f"<td style='padding:2px 8px;color:{color}'>{html.escape(detail)}</td></tr>")

        # SqlDBM: GET the OpenAPI doc and require HTTP 200 (clearer signal than a 401 on /projects)
        if sql_code == 200:
            sql_state, sql_text = "ok", "reachable — API serving (HTTP 200)"
        elif sql_reach:
            sql_state, sql_text = "warn", f"reachable but HTTP {sql_code} (proxy or redirect in path?)"
        else:
            sql_state, sql_text = "bad", "unreachable"
        # GitHub raw: require HTTP 200
        if gh_code == 200:
            gh_state, gh_text = "ok", "reachable (HTTP 200)"
        elif gh_reach:
            gh_state, gh_text = "warn", f"reachable but HTTP {gh_code}"
        else:
            gh_state, gh_text = "bad", gh_d

        rows = [
            line("info", "Compute", f"{compute} · DBR {dbr}",
                 "Cluster type and Databricks Runtime version. 'Classic cluster' = a standard "
                 "all-purpose / job cluster; 'Serverless / Spark Connect' = serverless compute. "
                 "This is NOT the metastore — Hive vs Unity Catalog is the Catalogs / UC row below."),
            line("ok" if uc else "warn", "Catalogs / UC",
                 f"{uc_note} · current='{cur}' · [{', '.join(cats) if cats else 'none'}]",
                 "Whether the workspace uses Unity Catalog or is Hive-metastore-only, plus the current "
                 "catalog and the full SHOW CATALOGS list."),
            line(sql_state, "SqlDBM API (api.sqldbm.com)", sql_text,
                 "GETs the SqlDBM OpenAPI doc (/swagger/v1/swagger.json) and expects HTTP 200, confirming "
                 "this cluster can reach the SqlDBM REST API."),
            line(gh_state, "Script host (raw.githubusercontent.com)", gh_text,
                 "Whether this cluster can fetch import.py from the GitHub raw URL the bootstrap uses."),
        ]
        notes = []
        if not uc:
            notes.append("Importing from the Hive metastore catalog — some legacy Hive-SerDe tables may "
                         "not emit DDL via SHOW CREATE TABLE and will be skipped.")
        if sql_state == "bad":
            notes.append("SqlDBM API unreachable — in locked-down / Azure Gov networks, allowlist "
                         "api.sqldbm.com on egress, and confirm sending DDL to the commercial endpoint "
                         "is permitted under your compliance boundary.")
        elif sql_state == "warn":
            notes.append(f"Reached api.sqldbm.com but got HTTP {sql_code} instead of 200 — an inspecting "
                         "proxy may be in the path, or the endpoint changed.")
        if gh_state == "bad":
            notes.append("GitHub raw unreachable — host import.py inside the workspace (Workspace file "
                         "or Repo) instead of fetching it from GitHub.")
        elif gh_state == "warn":
            notes.append(f"Reached raw.githubusercontent.com but got HTTP {gh_code} instead of 200 — "
                         "check the file path / branch in the bootstrap URL.")
        notes_html = ("<ul style='margin:6px 0 0 18px;font-size:12px;color:#555'>"
                      + "".join(f"<li>{html.escape(n)}</li>" for n in notes) + "</ul>") if notes else ""
        display(HTML(
            "<div style='font-family:sans-serif;font-size:13px'>"
            f"<table style='border-collapse:collapse'>{''.join(rows)}</table>{notes_html}</div>"))

preflight_btn.on_click(run_preflight)
run_preflight()   # auto-run once at load

_rows = [
    widgets.HTML("<h4 style='margin:4px 0'>Environment preflight</h4>"),
    preflight_btn, preflight_out,
    widgets.HTML("<hr style='margin:10px 0'>"),
]
for _h, _c in zip(step_headers, step_contents):
    _rows.append(_h)
    _rows.append(_c)
display(widgets.VBox(_rows))
