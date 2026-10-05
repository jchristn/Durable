namespace Durable
{
    using System;

    /// <summary>
    /// Features a repository backend supports. Query builders and <see cref="Durable.Query.RepositoryBase{T}"/> check
    /// them when a call is made, so an unsupported operation fails immediately with a <see cref="NotSupportedException"/>
    /// naming the capability instead of failing part-way through execution. Basic CRUD, filtering with comparisons and
    /// boolean logic, ordering, paging, counting, query filters and soft delete are always required and have no flag.
    /// </summary>
    [Flags]
    public enum RepositoryCapabilities : long
    {
        /// <summary>No optional features.</summary>
        None = 0,

        /// <summary>Explicit transactions (<see cref="IRepository{T}.BeginTransaction"/>) with commit and rollback.</summary>
        Transactions = 1,

        /// <summary>Include/ThenInclude of reference and collection navigations.</summary>
        Include = 2,

        /// <summary>Many-to-many navigations (in Include and in navigation predicates).</summary>
        ManyToMany = 4,

        /// <summary>Navigation members (<c>x.Author.Name</c>) and collection Any/All/Count in predicates.</summary>
        NavigationPredicates = 8,

        /// <summary>GroupBy with Having, group aggregates and grouped projections.</summary>
        Grouping = 16,

        /// <summary>Select projections into another type.</summary>
        Projection = 32,

        /// <summary>Distinct results.</summary>
        Distinct = 64,

        /// <summary>Sum, Average, Min and Max over a selector.</summary>
        Aggregates = 128,

        /// <summary>String, date and math functions in queries (ToUpper, Trim, Substring, Length, Year, Math.Round, ...).</summary>
        Functions = 256,

        /// <summary>Exact <see cref="StringMatchMode.Ordinal"/> and <see cref="StringMatchMode.IgnoreCase"/> string matching.</summary>
        StringMatchModes = 512,

        /// <summary>Entities with composite primary keys.</summary>
        CompositeKeys = 1024,

        /// <summary>Version columns with optimistic concurrency checks and conflict resolvers.</summary>
        OptimisticConcurrency = 2048,

        /// <summary>Set-based updates by predicate (<c>UpdateField</c>, <c>BatchUpdate</c>).</summary>
        BatchUpdate = 4096,

        /// <summary>Upsert (insert or update by key).</summary>
        Upsert = 8192,

        /// <summary>Every capability.</summary>
        All = Transactions | Include | ManyToMany | NavigationPredicates | Grouping | Projection | Distinct | Aggregates
            | Functions | StringMatchModes | CompositeKeys | OptimisticConcurrency | BatchUpdate | Upsert
    }
}
