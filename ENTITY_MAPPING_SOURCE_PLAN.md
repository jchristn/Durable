# Entity Mapping Source: Implementation Plan

**Status:** Not started
**Estimate:** 2 to 2.5 developer days
**Target version:** not assigned. A new public API is a MINOR change, so the proposal is `0.6.0-alpha`, but nobody changes a version number until the maintainer approves it (see `~/Code/Agents/requirements/VERSIONING.md`, section 4).

## How to use this file

Work the tasks in order; each one lists what it depends on. When you start a task, check its box with `[~]`, put your name next to **Owner**, and add a line to the progress log at the bottom. When the acceptance criteria are all met, change the box to `[x]` and log the commit hash. If you change the design, edit the design section here in the same commit, so the plan never describes something the code doesn't do. Blocked work gets `[!]` and a sentence in **Notes** saying what it is waiting on.

Box states: `[ ]` not started, `[~]` in progress, `[x]` done, `[!]` blocked.

## Why we're doing it

Durable maps an entity from its own attributes (`[Entity]`, `[Property]`, `[ForeignKey]` and friends) or, when a class has no `[Property]` attributes, from naming conventions. That leaves out classes that can't carry Durable's attributes: generated code, shared models owned by another team, and models that already use some other attribute system. The TernaryTech-io fork exists because of the third case. They added an `IEntityMetadataProvider` to the pre-0.3 per-provider code, then lost it when they merged 0.4.0 and the build broke.

Their version let each repository take its own provider. We are deliberately not doing that. Per-repository mapping would mean passing the provider through includes, the LINQ normalizer, migrations, the CLI and every non-SQL backend, and changing the metadata cache key everywhere; roughly two weeks of work with real risk of subtle include and migration bugs. A process-wide registration covers the actual need (map a class Durable doesn't own) for a fraction of the cost, and nothing stops us adding per-repository mapping later if someone shows up needing one class mapped two ways in one process.

## Design

Every attribute Durable reads for mapping is read in one place: the `EntityMetadata` constructor and its private helpers in `src/Durable/EntityMetadata.cs`. The engine, `IncludeLoader`, migrations and all backends consume the cached result of `EntityMetadata.For(Type)`. That is what makes this cheap. If we route those reads through an interface, every consumer gets the new behavior for free.

The interface returns the same attribute objects Durable already understands rather than introducing a builder. That keeps the project's "attribute-based configuration, no fluent API" principle intact and means an adapter is a translation table, not a second mapping language.

```csharp
namespace Durable
{
    public interface IEntityMappingSource
    {
        EntityAttribute? GetEntityAttribute(Type entityType);
        IEnumerable<Attribute> GetPropertyAttributes(Type entityType, PropertyInfo property);
        IEnumerable<CompositeIndexAttribute> GetCompositeIndexes(Type entityType);
    }
}

// Registration, once at startup, before the first repository for the type is used
DurableMapping.Register<Customer>(new TernaryMappingSource());
DurableMapping.MappingSource = new TernaryMappingSource();   // or: the default for every type not registered individually
```

The attributes `GetPropertyAttributes` may return are exactly the ones `EntityMetadata` reads today: `PropertyAttribute`, `ForeignKeyAttribute`, `NavigationPropertyAttribute`, `InverseNavigationPropertyAttribute`, `ManyToManyNavigationPropertyAttribute`, `NotMappedAttribute`, `ValueConverterAttribute`, `VersionColumnAttribute`, `SoftDeleteAttribute`, `IndexAttribute` and `DefaultValueAttribute`. Anything else is ignored.

Three rules keep the global state honest. Registering a source for a type whose metadata has already been built throws `InvalidOperationException`, because a type that silently switches mappings mid-process would corrupt cached SQL and materializers. Lookup order is per-type registration, then `DurableMapping.MappingSource`, then the built-in reflection source. Registration is thread-safe, but it is meant for startup and the XML docs say so.

## Tasks

### [ ] 1. Route attribute reads through an internal source

**Owner:** unassigned
**Depends on:** nothing
**Risk:** low
**Requirement source:** `CLAUDE.md` (code style, AOT rules), `~/Code/Agents/requirements/CODE_STYLE.md`

Add an internal `ReflectionMappingSource` that does exactly what the code does today, and change every `GetCustomAttribute` call in `EntityMetadata.cs` to go through it: the constructor (entity attribute, the convention-mapping check, key order, version column info, composite indexes), `BuildNavigation`, `BuildColumn` and `BuildDefaultValue`. No public API yet and no behavior change. Its whole point is to leave a single seam that step 2 can open.

**Acceptance criteria:**
- [ ] No `GetCustomAttribute` call remains in `EntityMetadata.cs` outside `ReflectionMappingSource`.
- [ ] Every suite passes unchanged on SQLite, PostgreSQL, MySQL and SQL Server, on net8.0 and net10.0.
- [ ] `Test.Aot` publishes with zero warnings and its binary passes.

**Notes:**

### [ ] 2. Public API: `IEntityMappingSource` and registration

**Owner:** unassigned
**Depends on:** 1
**Risk:** medium (global state, ordering)
**Requirement source:** `CLAUDE.md` (public API conventions, AOT annotations, XML docs incl. thread safety)

Make the interface public and add `DurableMapping.MappingSource` and `DurableMapping.Register<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T>(IEntityMappingSource source)` with a matching `Register(Type, IEntityMappingSource)`. Guard against null, enforce the already-built rule with a message that names the type, and document the lookup order, thread safety and the startup-only expectation in XML docs.

**Acceptance criteria:**
- [ ] Registering after `EntityMetadata.For<T>()` throws `InvalidOperationException` naming the type.
- [ ] `PublicApiConventionsTestSuite` passes with no new allow-list entries.
- [ ] Build has zero warnings, including trim/AOT analyzers.

**Notes:**

### [ ] 3. Tests across every backend

**Owner:** unassigned
**Depends on:** 2
**Risk:** medium (global registration must not leak between suites)
**Requirement source:** `CLAUDE.md` (testing strategy, conformance rule), `~/Code/Agents/requirements/BACKEND_TEST_ARCHITECTURE.md`

Add test-only attributes (say `[MapTable]`, `[MapColumn]`, `[MapKey]`) and an adapter that translates them, then a `MappingSourceTestSuite` in `Test.Shared` over entities that carry no Durable attributes at all. Each test uses its own entity types so registration can't bleed into another suite. Cover CRUD; composite and auto-increment keys; reference, collection and many-to-many includes; a version column with a conflict; soft delete; a value converter; indexes created by `InitializeTable`; and a migration diff that comes back empty after sync. Register it in `DurableTestSuites` and run the in-memory and LiteDB backends against the same entities.

**Acceptance criteria:**
- [ ] The suite passes on all four SQL providers and on net8.0 and net10.0.
- [ ] The same entities pass through the in-memory and LiteDB backends.
- [ ] `Test.Aot` gains a check that a registered type round-trips under Native AOT, and the binary passes.
- [ ] Full suite run shows no change in pass counts elsewhere.

**Notes:**

### [ ] 4. `durable` CLI support

**Owner:** unassigned
**Depends on:** 2
**Risk:** low
**Requirement source:** `CLAUDE.md` (Durable.Tool)

The CLI finds entities by scanning for `[Entity]`, so registered types are invisible to `migrate`, `schema` and `scaffold`. Add a `--mapping-source <TypeName>` option that instantiates the source from the user's assembly and registers it before scanning. The scanner then also needs to accept types the source returns an `EntityAttribute` for. If time is short, ship without it and document the limitation in the README; mark this task `[!]` with that decision in the notes.

**Acceptance criteria:**
- [ ] `DurableToolTestSuite` covers a migration generated for a registered type.
- [ ] `durable --help` lists the option.

**Notes:**

### [ ] 5. Documentation and release preparation

**Owner:** unassigned
**Depends on:** 3 (and 4, or its documented deferral)
**Risk:** low
**Requirement source:** `~/Code/Agents/requirements/WRITING_DOCUMENTS.md`, `VERSIONING.md`, `REPOSITORY_REQUIREMENTS.md`

Add a README section with a complete adapter example and the registration rules, a CHANGELOG entry, and a line in `CLAUDE.md` under Attributes noting that mapping can come from an `IEntityMappingSource`. No em-dashes anywhere. Ask the maintainer for the version number before touching any project file.

**Acceptance criteria:**
- [ ] README, CHANGELOG and CLAUDE.md updated and accurate.
- [ ] Version number approved by the maintainer and recorded in the progress log before it is applied.

**Notes:**

## Out of scope

Per-repository mapping, changing a type's mapping after first use, and a fluent configuration API. Each is a reasonable future request; none is needed to unblock the case that prompted this work.

## Progress log

| Date | Task | Who | Change | Commit |
| --- | --- | --- | --- | --- |
| 2026-10-06 | n/a | Claude | Plan written | uncommitted |
