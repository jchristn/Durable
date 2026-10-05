namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// The backend under test. Implement this once for an <see cref="IRepository{T}"/> backend (SQL database, document
    /// store, search engine, graph store, in-memory store, ...) and pass it to <see cref="ConformanceSuites.Build"/>.
    /// <para>
    /// The kit only talks to the backend through <see cref="IRepository{T}"/>, <see cref="IQueryBuilder{T}"/>,
    /// <see cref="IGroupedQueryBuilder{T, TKey}"/> and <see cref="ITransaction"/>; this interface adds the few things the
    /// kit cannot do through those contracts: creating repositories and resetting storage between tests.
    /// </para>
    /// <para>
    /// Storage: the kit's entity types (see <see cref="ConformanceEntities.All"/>) carry their own table/collection
    /// names (<see cref="EntityMetadata.TableName"/>, all prefixed <c>cf_</c> except the convention-mapped entity).
    /// Repositories created by the same target must share storage, so rows written through one repository are visible to
    /// another repository of the same or a related entity type (navigations are resolved across repositories), and an
    /// <see cref="ITransaction"/> started on one repository can be passed to any other repository of the same target.
    /// </para>
    /// Thread safety: the kit runs its cases sequentially and never calls a target concurrently.
    /// </summary>
    public interface IConformanceTarget
    {
        /// <summary>
        /// Gets the display name of the backend, used in suite names and failure messages (for example "SQLite" or
        /// "MongoDB 8"). Never null.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the optional features the backend supports. Must equal <see cref="IRepository{T}.Capabilities"/> of the
        /// repositories this target creates. Cases that need a capability the target lacks are reported as skipped and the
        /// capability suite instead verifies that the corresponding operations throw <see cref="NotSupportedException"/> at
        /// the call site. Read once when the suites are built.
        /// </summary>
        RepositoryCapabilities Capabilities { get; }

        /// <summary>
        /// Creates a repository for <typeparamref name="T"/> over the target's shared storage. The caller disposes it.
        /// Creating a repository must not create, clear or otherwise touch storage; the kit calls
        /// <see cref="ResetAsync"/> first. A backend that cannot handle an entity shape at all (for example a composite key
        /// without <see cref="RepositoryCapabilities.CompositeKeys"/>) may throw <see cref="NotSupportedException"/> here.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="options">Repository options (for example <see cref="RepositoryOptions.StringMatching"/>); null uses the backend's defaults.</param>
        /// <returns>A new repository. Never null.</returns>
        /// <exception cref="NotSupportedException">Thrown when the backend cannot serve the entity type or the options.</exception>
        IRepository<T> CreateRepository<T>(RepositoryOptions? options = null) where T : class, new();

        /// <summary>
        /// Drops and recreates (or clears) the storage of the given entity types so that each one exists and is empty.
        /// SQL targets drop and recreate tables; document stores drop collections or delete all documents; in-memory
        /// targets clear their tables. Storage of other types must be left untouched. Generated keys are not required to
        /// restart, and the kit never assumes specific key values. Each case calls this before using any storage.
        /// </summary>
        /// <param name="entityTypes">Entity types whose storage is reset. Never null; never contains null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task that completes when the storage is ready.</returns>
        Task ResetAsync(IReadOnlyList<Type> entityTypes, CancellationToken token = default);
    }
}
