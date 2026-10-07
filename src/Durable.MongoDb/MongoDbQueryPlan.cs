namespace Durable.MongoDb
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Describes how a <see cref="MongoDbBackend"/> executed one read or write: which parts of the filter were pushed down to
    /// MongoDB as query filters (where MongoDB can use indexes), whether ordering, paging, counting or the aggregate ran on
    /// the server, and whether anything was evaluated client-side afterwards. Pushed filters are always necessary conditions
    /// of the query, so client-side evaluation can only remove documents; when <see cref="Exact"/> is true the pushed
    /// filters are the whole filter. Observe plans with <see cref="MongoDbBackend.QueryPlanned"/>,
    /// <see cref="MongoDbBackend.LastQueryPlan"/> or the backend's logger (Debug level).
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    public sealed class MongoDbQueryPlan : EventArgs
    {
        #region Public-Members

        /// <summary>
        /// Gets the operation: Query, Count, Aggregate, Replace, Update or Delete. Never null.
        /// </summary>
        public string Operation { get; }

        /// <summary>
        /// Gets the entity type read or written. Never null.
        /// </summary>
        public Type EntityType { get; }

        /// <summary>
        /// Gets the MongoDB collection. Never null.
        /// </summary>
        public string Collection { get; }

        /// <summary>
        /// Gets the filters pushed down to MongoDB (ANDed), as relaxed extended JSON. Never null; empty when every document
        /// of the collection was read.
        /// </summary>
        public IReadOnlyList<string> PushedFilters { get; }

        /// <summary>
        /// Gets the sort sent to MongoDB as relaxed extended JSON, or null when MongoDB did not sort.
        /// </summary>
        public string? PushedSort { get; }

        /// <summary>
        /// Gets the aggregation pipeline sent to MongoDB as relaxed extended JSON (aggregates only), or null.
        /// </summary>
        public string? Pipeline { get; }

        /// <summary>
        /// Gets whether the pushed filters are exactly the filter (nothing was filtered client-side).
        /// </summary>
        public bool Exact { get; }

        /// <summary>
        /// Gets the client-side part of the query (residual filter, ordering, paging, aggregate) in Durable's neutral
        /// notation, or null when MongoDB evaluated everything.
        /// </summary>
        public string? ClientSide { get; }

        /// <summary>
        /// Gets whether Skip/Take (or counting, or the aggregate) ran inside MongoDB.
        /// </summary>
        public bool PagingPushedDown { get; }

        /// <summary>
        /// Gets the number of documents MongoDB returned (before client-side evaluation); -1 when MongoDB only counted,
        /// aggregated or wrote.
        /// </summary>
        public long DocumentsRead { get; }

        /// <summary>
        /// Gets MongoDB's winning plan (from the <c>explain</c> command, query planner verbosity) as relaxed extended JSON
        /// when <see cref="MongoDbBackend.ExplainQueries"/> is enabled and the operation is a find; otherwise null.
        /// </summary>
        public string? MongoDbExplain { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a plan.
        /// </summary>
        /// <param name="operation">Operation. Must not be null.</param>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="collection">Collection. Must not be null.</param>
        /// <param name="pushedFilters">Pushed filters. Must not be null.</param>
        /// <param name="pushedSort">Pushed sort; may be null.</param>
        /// <param name="pipeline">Aggregation pipeline; may be null.</param>
        /// <param name="exact">Whether the pushed filters are the whole filter.</param>
        /// <param name="clientSide">Client-side part; may be null.</param>
        /// <param name="pagingPushedDown">Whether paging, counting or the aggregate ran in MongoDB.</param>
        /// <param name="documentsRead">Documents returned by MongoDB, or -1.</param>
        /// <param name="mongoDbExplain">Winning plan JSON; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when operation, entityType, collection or pushedFilters is null.</exception>
        public MongoDbQueryPlan(string operation, Type entityType, string collection, IReadOnlyList<string> pushedFilters, string? pushedSort, string? pipeline, bool exact, string? clientSide, bool pagingPushedDown, long documentsRead, string? mongoDbExplain)
        {
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            Collection = collection ?? throw new ArgumentNullException(nameof(collection));
            PushedFilters = pushedFilters ?? throw new ArgumentNullException(nameof(pushedFilters));
            PushedSort = pushedSort;
            Pipeline = pipeline;
            Exact = exact;
            ClientSide = clientSide;
            PagingPushedDown = pagingPushedDown;
            DocumentsRead = documentsRead;
            MongoDbExplain = mongoDbExplain;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string ToString()
        {
            string pushed = PushedFilters.Count == 0 ? "(all documents)" : string.Join(" AND ", PushedFilters);
            return Operation + " " + Collection + " WHERE " + pushed
                + (PushedSort != null ? " SORT " + PushedSort : string.Empty)
                + (Pipeline != null ? " PIPELINE " + Pipeline : string.Empty)
                + (PagingPushedDown ? " [paging in MongoDB]" : string.Empty)
                + (ClientSide != null ? " | client: " + ClientSide : " | exact")
                + (DocumentsRead >= 0 ? " | read " + DocumentsRead : string.Empty);
        }

        #endregion
    }
}
