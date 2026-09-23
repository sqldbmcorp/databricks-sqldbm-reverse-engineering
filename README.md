# Databricks → SqlDBM DDL Importer

An interactive Databricks notebook that reads table/view DDL straight from your
**Unity Catalog or Hive metastore** and pushes it into **SqlDBM** through the public
REST API — as a new project, a new revision on an existing project, or a revision on a
Concurrent-Working branch.

The whole experience runs from a single notebook cell: a three-line bootstrap fetches
`import.py` from this repo and renders a guided, four-step form.

---

## Quick start

Paste this into a notebook cell and run it:

```python
import urllib.request
url = "https://raw.githubusercontent.com/sqldbmcorp/databricks-sqldbm-reverse-engineering/refs/heads/main/import.py"
exec(compile(urllib.request.urlopen(url).read().decode(), "import.py", "exec"), globals())
```

`exec(..., globals())` runs the script in the notebook's namespace so it inherits `spark`
and renders the widgets inline. Then:

1. Check the **Environment preflight** banner at the top — all four rows should be green
   (Compute is informational). Hover any row label for an explanation.
2. **Step 1 · Select Catalog and Schema(s)** — pick a catalog (none is pre-selected), then
   schema(s), optionally a *Name filter* and object types, and click *List Objects*. Catalogs marked **⚠ foreign** are Lakehouse
   Federation sources — see [Foreign catalogs](#foreign-lakehouse-federation-catalogs).
3. **Step 2 · Select Objects and Generate DDL** — review the object list; uncheck anything
   you don't want; click *Generate DDL for Selected*.
4. **Step 3 · DDL Confirmation** — review the DDL, then click *Confirm & Configure
   Destination*.
5. **Step 4 · Configure Destination Project** — paste your API token, click *Connect &
   load projects*, choose your destination, and click *Submit to SqlDBM*.

> GitHub's raw CDN caches for a few minutes, so a freshly pushed change to `import.py`
> may take a moment to appear. During development you can append a cache-buster, e.g.
> `url + f"?v={time.time()}"`.

---

## Why this exists

SqlDBM customers frequently already have their schemas defined in Databricks and want to
reverse-engineer them into a SqlDBM data model. The manual path is tedious — export DDL,
clean it up, paste or upload it, repeat per schema. Network security requirements often
prevent SqlDBM users from connecting to their Databricks environment directly from the application, 
so an in-app reverse-engineer isn't always an option. This notebook automates that round trip — running
the extraction inside the cluster, where the data already is — and keeps a human in the loop
where it matters:

- **Pulls DDL where it lives.** Reads directly from the catalog/metastore on the cluster,
  so there's no intermediate file to manage or lose.
- **Object-level control.** You pick the catalog and schema(s), then review and de-select
  individual objects before anything is sent.
- **Pushes via the SqlDBM API.** Creates a project, a revision on the latest, a revision on
  a chosen revision, or routes through a branch for Concurrent-Working projects.
- **Consistent with the SqlDBM app.** DDL comes from the same `SHOW CREATE TABLE` /
  `SHOW CREATE FUNCTION` statements SqlDBM's own reverse-engineering tool uses, so the
  generated DDL matches what the product would produce. (Object *names* are enumerated with
  `SHOW TABLES` / `SHOW VIEWS` rather than `listTables`, which scales to very large schemas.)
- **Honest about restricted environments.** A built-in preflight reports compute type,
  Unity Catalog vs Hive metastore, and endpoint reachability, so users on locked-down or
  government clouds see what will and won't work before they start.

---

## What it does

When the bootstrap runs, the notebook renders an **Environment preflight** banner followed
by a four-step accordion (only one step open at a time):

1. **Select Catalog and Schema(s)** — choose a catalog, select one or more schemas,
   optionally enter a **Name filter** and choose which object types to include (Tables,
   Views, Functions), and click *List Objects*. No catalog is selected on load, so nothing
   is queried until you pick one. Foreign catalogs are flagged and never queried
   automatically.
2. **Select Objects and Generate DDL** — discovered objects appear as checkboxes, grouped
   by schema and **paginated** (100 / 250 / 500 per page). Filter by name, *Select all
   matching* / *Deselect all matching* / *Select this page*, then click *Generate DDL for
   Selected* to build the DDL payload.
3. **DDL Confirmation** — review the exact DDL that will be sent, then click
   *Confirm & Configure Destination*.
4. **Configure Destination Project** — enter your SqlDBM API token, click
   *Connect & load projects*, choose a destination (new project or existing), branch (for
   Concurrent-Working projects), and revision target, then click *Submit to SqlDBM*. On
   success you get direct links to the project (and branch, when applicable).

---

## Prerequisites

**SqlDBM**

- A **Standard Enterprise** SqlDBM account.
- **API access enabled** for your account (request it via your Account Manager or a support
  ticket).
- A **personal SqlDBM API token with GET+POST scope** — the importer both reads your projects and
  branches and writes revisions, so it needs both. Generate one under **Account → App Tokens**.

**Databricks**

- A workspace with **Unity Catalog** or a **Hive metastore** (both are supported).
- **Databricks Runtime 13.x or newer** recommended — the extraction uses
  `spark.catalog.setCurrentCatalog`, `listTables`, and `listDatabases`, which require
  Spark 3.4+. (This is the same baseline SqlDBM's own tool requires.)
- **`ipywidgets`**, available on current DBR and serverless compute.
- **Unity Catalog / metastore read access** to the schemas you want to import.
- **Outbound HTTPS** from the cluster to `api.sqldbm.com` and (for the bootstrap)
  `raw.githubusercontent.com`.

---

## The environment preflight

The preflight runs automatically on load (and has a *Re-run environment checks* button). It
reports:

- **Compute** — cluster type (*Classic cluster* vs *Serverless / Spark Connect*) and the
  Databricks Runtime version. This is the compute, **not** the metastore.
- **Catalogs / UC** — whether Unity Catalog is enabled or the workspace is
  Hive-metastore-only, plus the full `SHOW CATALOGS` list.
- **SqlDBM API** — GETs `https://api.sqldbm.com/swagger/v1/swagger.json` and expects
  HTTP 200, confirming the cluster can reach the SqlDBM REST API.
- **Script host** — confirms the cluster can fetch `import.py` from GitHub. It probes the
  URL your bootstrap cell used (the `url` variable), falling back to this repo's `main`.

It also lists any foreign (Lakehouse Federation) catalogs it finds.

Any non-passing check prints a plain-language note explaining how to remediate it.

---

## Large schemas

The importer is built to handle schemas with tens of thousands of objects:

- **Filter before you list.** Step 1's *Name filter* is sent to Databricks as
  `SHOW TABLES … LIKE '<pattern>'`, so only matching names come back. Patterns are
  case-insensitive; `*` matches any characters and `|` separates alternatives — e.g.
  `fact_*|dim_*` or `*_2024*`. Untick *Views* or *Functions* to skip those types.
- **Fast enumeration.** Names come from one `SHOW TABLES` and one `SHOW VIEWS` per schema,
  not per-table metadata lookups. Exact kinds (streaming table, materialized view) are
  confirmed once DDL is generated.
- **Paginated selection.** Step 2 renders one page at a time; selection is tracked
  separately, so *Select all matching* covers every match, not just the visible page.
- **Nothing pre-selected above 500 objects**, so a large listing can't accidentally turn
  into tens of thousands of `SHOW CREATE` calls. Generating DDL for more than 1,000 objects
  asks for a second click and shows progress with an estimated time remaining.
- **Parallel, cancellable DDL generation.** `SHOW CREATE` runs on a pool of worker threads
  (*Parallel*: 1 / 4 / 8 / 16, default 8) in the background, so the notebook stays
  responsive. **Cancel** stops queued work immediately and, on serverless / Spark Connect,
  interrupts in-flight queries. DDL already generated is kept, so clicking *Generate* again
  resumes with the rest. If your compute throttles concurrent metadata queries, set
  *Parallel* to 1.
- **Step 3 previews the first 200 objects** and reports the full payload size.

---

## Foreign (Lakehouse Federation) catalogs

A **foreign catalog** mirrors an external database (SQL Server, PostgreSQL, Snowflake, …)
through a Unity Catalog *connection*. Its schemas and tables are **not stored in Unity
Catalog**: listing schemas, listing tables and `SHOW CREATE TABLE` are all sent *live* to
the external database **from the compute running the notebook**. If that compute can't
reach the database, Step 1 fails with a JDBC error such as
`The TCP/IP connection to the host … has failed`, even though the catalog shows up in
`SHOW CATALOGS`.

The importer flags these catalogs as **⚠ foreign (federated)** and does not query them
until you click *Try loading schemas anyway*. To make them work:

1. **Check the connection.** Catalog Explorer → External data → Connections → *your
   connection* → **Test connection**. Verify host, port and credentials (SQL Server's default
   TCP port is 1433; 1434 is normally the SQL Browser / DAC port).
2. **Give the compute a network path to the database.**
   - *Serverless compute* runs in the Databricks-managed network, not your VNet/VPC. An
     account admin creates a **Network Connectivity Configuration (NCC)**, attaches it to the
     workspace, and either adds a **private endpoint rule** to the database (Azure Private
     Link / AWS PrivateLink) or allowlists the NCC's **stable egress IPs** on the database
     firewall.
   - *Classic compute:* run the notebook on a Unity Catalog–enabled all-purpose cluster
     (Standard/Shared or Dedicated access mode, DBR 13.3 LTS+) deployed in a VNet/VPC that
     can route to the database, with the database firewall allowing that subnet.
3. **Confirm permissions:** `USE CATALOG` on the catalog, `USE SCHEMA` and `SELECT` on the
   schemas you import.
4. **Test from a cell:** ``SHOW SCHEMAS IN `<catalog>` `` — once it returns, load the schemas in
   Step 1.
5. **Or reverse-engineer the source directly.** DDL read through a foreign catalog uses
   Databricks' mapped types, not the source's native DDL. For a native model, create a SqlDBM
   project with the source database type (e.g. SQL Server) and reverse-engineer from that
   database directly.

---

## Concurrent Working (branches)

For Concurrent-Working (CW) projects, SqlDBM doesn't allow writing to `main` directly. The
form detects CW (more than one branch, or any non-`main` branch) and reveals a **Branch**
picker: choose an existing branch, or create a new one (pre-named
`databricks-import/<user>-<timestamp>`). Submissions are then routed through the
branch-scoped API endpoints, and the success message links to both `main` and the target
branch.

---

## Security & tokens

- The API token is entered in a **masked** field and is used only to call the SqlDBM API
  from your cluster. It is not written to disk or logged by the script.
- The SqlDBM API token governs what projects the user can push to; the importer can only
  do what the token is scoped for.
