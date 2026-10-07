# docs-fragments (v0.7.0 working folder, deleted before release)

Several agents build v0.7.0 in parallel. To keep `README.md`, `CHANGELOG.md`, `CLAUDE.md` and `src/CLAUDE.md` free of
merge conflicts, no agent edits those files. Each agent instead commits exactly one file here, named after itself:

```
docs-fragments/<agent-name>.md      (for example oracle.md, duckdb.md, wire-compat.md, mongodb.md, cosmosdb.md)
```

The integration step folds every fragment into the real documents, then deletes this folder (`git rm -r docs-fragments`)
before the release commit. Nothing in this folder ships, and nothing may link to it.

## What a fragment contains

Use these four headings, in this order, so integration can fold fragments mechanically:

1. `## README sections`: the section(s) to add, in final wording, with code examples that you have compiled. Say where
   each section goes (for example "after the PostgreSQL section").
2. `## README table rows`: rows for the existing tables (packages, supported databases and versions, feature matrix,
   `durable` CLI provider names, troubleshooting), one table per sub-heading, written as Markdown table rows.
3. `## CHANGELOG`: v0.7.0 bullets grouped by package (`### Durable.Oracle`, `### Durable.MySql`, ...).
4. `## CLAUDE.md`: lines for the project tree and the notes (the same text goes into `CLAUDE.md` and `src/CLAUDE.md`,
   which must stay identical).

Also list documented limitations and capability gates (what is gated, why, and the README wording).

## Rules

- No em-dashes (U+2014) anywhere; use commas, colons, parentheses or periods.
- Version is 0.7.0 (no suffix).
- Keep it factual: driver package names and versions, tested database images and versions, the exact `--type` name.

## Test wiring seams (from the scaffolding change)

The shared test plumbing already knows every v0.7.0 target, so each agent fills in its own placeholders instead of
editing shared switches. Placeholders throw `NotSupportedException("... is added by a later v0.7.0 change")` from
`TestDatabaseTypes.NotYetAvailable`.

SQL targets (Oracle, DuckDB, MariaDB, CockroachDB, YugabyteDB):

1. `src/Test.Shared/RepositoryProviderFactory.cs`: the bodies of `Create<Target>Provider` and
   `Build<Target>ConnectionString`. MariaDB reuses `new MySqlRepositoryProvider(cs, TestDatabaseType.MariaDb)`;
   CockroachDB / YugabyteDB reuse `new PostgresRepositoryProvider(cs, TestDatabaseType.CockroachDb | YugabyteDb)`.
2. `src/Test.Automated/ProviderDockerSettings.cs`: the body of `Create<Target>` (not DuckDB: it is in-process, like
   SQLite). Optional `ExtraRunArguments` (for example `--memory`), `ContainerCommand`, `StartupTimeout` and
   `ReadinessProbe`; without a probe, readiness is `RepositoryProviderFactory.Create` +
   `IRepositoryProvider.IsDatabaseAvailableAsync`, retried until the timeout.
3. `src/Test.Shared/Test.Shared.csproj`: your labeled `<ItemGroup Label="<Target>">`.
4. Provider-specific suites: your case in `DurableTestSuites.AddTargetSpecificSuites`.
5. Suite switches: MariaDB is already listed next to `MySql`, CockroachDB / YugabyteDB next to `Postgres`, and suite
   gating uses `TestDatabaseTypes.IsMySqlFamily` / `IsPostgresFamily`. Split your label out only where your database
   differs. New providers add their `case` right after `Sqlite` (DuckDB) or right after `SqlServer` (Oracle), so the
   two agents never touch adjacent lines.
6. `durable` CLI (`src/Durable.Tool/DatabaseTarget.cs`): one line in `_CanonicalProviders` (DuckDB after "sqlite",
   Oracle after "sqlserver"), a `case` in `Create` and in `NormalizeProvider`. Help and error texts read the list.
7. CI: replace your `# <name>` placeholder in the `databases` matrix of `.github/workflows/ci.yml` with `- <name>`.

Document backends (MongoDB, Cosmos DB):

1. A class implementing `src/Test.Shared/IDocumentBackendTestTarget.cs` (constructor takes `TestRuntimeConfiguration`
   and must not connect; `InitializeAsync` connects and is idempotent; `ConformanceTarget`; `BuildBackendSuites`).
2. One line in `src/Test.Shared/DocumentBackendTestTargets.cs`: replace your `throw` with
   `return new <Backend>TestTarget(configuration);`.
3. `src/Test.Automated/ProviderDockerSettings.cs`: the body of `Create<Backend>`. Readiness defaults to
   `DocumentBackendTestTargets.ProbeAsync` (create, initialize, dispose), so no probe is needed unless you want one.
4. `src/Test.Shared/Test.Shared.csproj`: your labeled `ItemGroup`; CI: your `# <name>` placeholder.

With `--type mongodb|cosmosdb`, `DurableTestSuites.All` builds no SQL suites: it runs the conformance kit
(`Conformance.*`) against your target, your backend suites, and the backend-neutral suites every configuration runs.
