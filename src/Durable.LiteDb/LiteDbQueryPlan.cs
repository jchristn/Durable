namespace Durable.LiteDb
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Describes how a <see cref="LiteDbBackend"/> executed one read or write: which parts of the filter were pushed down to
    /// LiteDB as <c>BsonExpression</c> predicates (where LiteDB can use indexes), whether anything was evaluated client-side
    /// afterwards, and whether paging or counting ran inside LiteDB. Pushed predicates are always necessary conditions of the
    /// query, so client-side evaluation can only remove documents; when <see cref="Exact"/> is true the pushed predicates are
    /// the whole filter. Observe plans with <see cref="LiteDbBackend.QueryPlanned"/>, <see cref="LiteDbBackend.LastQueryPlan"/>
    /// or the backend's logger (Debug level).
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    public sealed class LiteDbQueryPlan : EventArgs
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
        /// Gets the LiteDB collection. Never null.
        /// </summary>
        public string Collection { get; }

        /// <summary>
        /// Gets the predicates pushed down to LiteDB (ANDed), with parameters shown as <c>@pN</c>. Never null; empty when
        /// every document of the collection was read.
        /// </summary>
        public IReadOnlyList<string> PushedPredicates { get; }

        /// <summary>
        /// Gets the LiteDB parameters of the pushed predicates as JSON, or null when there are none.
        /// </summary>
        public string? Parameters { get; }

        /// <summary>
        /// Gets whether the pushed predicates are exactly the filter (nothing was evaluated client-side).
        /// </summary>
        public bool Exact { get; }

        /// <summary>
        /// Gets the client-side part of the query (residual filter, ordering, paging) in Durable's neutral notation, or
        /// null when LiteDB evaluated everything.
        /// </summary>
        public string? ClientSide { get; }

        /// <summary>
        /// Gets whether Skip/Take (or counting) ran inside LiteDB.
        /// </summary>
        public bool PagingPushedDown { get; }

        /// <summary>
        /// Gets the number of documents LiteDB returned (before client-side evaluation); -1 when LiteDB only counted.
        /// </summary>
        public long DocumentsRead { get; }

        /// <summary>
        /// Gets LiteDB's own plan (index choice and cost, from <c>ILiteQueryable.GetPlan</c>) as JSON when
        /// <see cref="LiteDbBackend.ExplainQueries"/> is enabled; otherwise null.
        /// </summary>
        public string? LiteDbExplain { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a plan.
        /// </summary>
        /// <param name="operation">Operation. Must not be null.</param>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="collection">Collection. Must not be null.</param>
        /// <param name="pushedPredicates">Pushed predicates. Must not be null.</param>
        /// <param name="parameters">Parameters as JSON; may be null.</param>
        /// <param name="exact">Whether the pushed predicates are the whole filter.</param>
        /// <param name="clientSide">Client-side part; may be null.</param>
        /// <param name="pagingPushedDown">Whether paging or counting ran in LiteDB.</param>
        /// <param name="documentsRead">Documents returned by LiteDB, or -1.</param>
        /// <param name="liteDbExplain">LiteDB plan JSON; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when operation, entityType, collection or pushedPredicates is null.</exception>
        public LiteDbQueryPlan(string operation, Type entityType, string collection, IReadOnlyList<string> pushedPredicates, string? parameters, bool exact, string? clientSide, bool pagingPushedDown, long documentsRead, string? liteDbExplain)
        {
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            Collection = collection ?? throw new ArgumentNullException(nameof(collection));
            PushedPredicates = pushedPredicates ?? throw new ArgumentNullException(nameof(pushedPredicates));
            Parameters = parameters;
            Exact = exact;
            ClientSide = clientSide;
            PagingPushedDown = pagingPushedDown;
            DocumentsRead = documentsRead;
            LiteDbExplain = liteDbExplain;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string ToString()
        {
            string pushed = PushedPredicates.Count == 0 ? "(all documents)" : string.Join(" AND ", PushedPredicates);
            return Operation + " " + Collection + " WHERE " + pushed
                + (Parameters != null ? " " + Parameters : string.Empty)
                + (PagingPushedDown ? " [paging in LiteDB]" : string.Empty)
                + (ClientSide != null ? " | client: " + ClientSide : " | exact")
                + (DocumentsRead >= 0 ? " | read " + DocumentsRead : string.Empty);
        }

        #endregion
    }
}
