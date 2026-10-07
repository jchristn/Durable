namespace Durable.CosmosDb
{
    using System;
    using System.Globalization;

    /// <summary>
    /// Describes how a <see cref="CosmosDbBackend"/> executed one read or write: the parameterized Cosmos DB SQL query it
    /// sent (or the point read it used), whether the query ran in one partition, whether anything was evaluated client-side
    /// afterwards, whether ordering and paging ran inside Cosmos DB, how many documents Cosmos DB returned and the request
    /// units charged. Pushed conditions are always necessary conditions of the query, so client-side evaluation can only
    /// remove documents; when <see cref="Exact"/> is true the pushed conditions are the whole filter. Observe plans with
    /// <see cref="CosmosDbBackend.QueryPlanned"/>, <see cref="CosmosDbBackend.LastQueryPlan"/> or the backend's logger
    /// (Debug level).
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    public sealed class CosmosDbQueryPlan : EventArgs
    {
        #region Public-Members

        /// <summary>
        /// Gets the operation: Query, Count, Aggregate, Replace, Update, Delete or Lookup (related documents). Never null.
        /// </summary>
        public string Operation { get; }

        /// <summary>
        /// Gets the entity type read or written. Never null.
        /// </summary>
        public Type EntityType { get; }

        /// <summary>
        /// Gets the Cosmos DB container. Never null.
        /// </summary>
        public string Container { get; }

        /// <summary>
        /// Gets the Cosmos DB SQL query text with parameters shown as <c>@pN</c>, or null for a point read.
        /// </summary>
        public string? QueryText { get; }

        /// <summary>
        /// Gets the query parameters as JSON, or null when there are none.
        /// </summary>
        public string? Parameters { get; }

        /// <summary>
        /// Gets the partition key the query or point read was scoped to (as JSON, for example <c>["tenant-1"]</c>), or null
        /// for a cross-partition query.
        /// </summary>
        public string? PartitionKey { get; }

        /// <summary>
        /// Gets whether the document was read by id and partition key (a point read) instead of a query.
        /// </summary>
        public bool PointRead { get; }

        /// <summary>
        /// Gets whether the pushed conditions are exactly the filter (nothing was evaluated client-side).
        /// </summary>
        public bool Exact { get; }

        /// <summary>
        /// Gets the client-side part of the query (residual filter, ordering, paging) in Durable's neutral notation, or null
        /// when Cosmos DB evaluated everything.
        /// </summary>
        public string? ClientSide { get; }

        /// <summary>
        /// Gets whether ORDER BY ran inside Cosmos DB.
        /// </summary>
        public bool OrderingPushedDown { get; }

        /// <summary>
        /// Gets whether OFFSET/LIMIT (or counting) ran inside Cosmos DB.
        /// </summary>
        public bool PagingPushedDown { get; }

        /// <summary>
        /// Gets the number of documents Cosmos DB returned (before client-side evaluation); -1 when Cosmos DB only counted or
        /// aggregated.
        /// </summary>
        public long DocumentsRead { get; }

        /// <summary>
        /// Gets the request units Cosmos DB charged for the operation's reads.
        /// </summary>
        public double RequestCharge { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a plan.
        /// </summary>
        /// <param name="operation">Operation. Must not be null.</param>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="container">Container. Must not be null.</param>
        /// <param name="queryText">Query text, or null for a point read.</param>
        /// <param name="parameters">Parameters as JSON; may be null.</param>
        /// <param name="partitionKey">Partition key as JSON; null for a cross-partition query.</param>
        /// <param name="pointRead">Whether a point read was used.</param>
        /// <param name="exact">Whether the pushed conditions are the whole filter.</param>
        /// <param name="clientSide">Client-side part; may be null.</param>
        /// <param name="orderingPushedDown">Whether ORDER BY ran in Cosmos DB.</param>
        /// <param name="pagingPushedDown">Whether paging or counting ran in Cosmos DB.</param>
        /// <param name="documentsRead">Documents returned by Cosmos DB, or -1.</param>
        /// <param name="requestCharge">Request units charged.</param>
        /// <exception cref="ArgumentNullException">Thrown when operation, entityType or container is null.</exception>
        public CosmosDbQueryPlan(string operation, Type entityType, string container, string? queryText, string? parameters, string? partitionKey, bool pointRead, bool exact, string? clientSide, bool orderingPushedDown, bool pagingPushedDown, long documentsRead, double requestCharge)
        {
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            Container = container ?? throw new ArgumentNullException(nameof(container));
            QueryText = queryText;
            Parameters = parameters;
            PartitionKey = partitionKey;
            PointRead = pointRead;
            Exact = exact;
            ClientSide = clientSide;
            OrderingPushedDown = orderingPushedDown;
            PagingPushedDown = pagingPushedDown;
            DocumentsRead = documentsRead;
            RequestCharge = requestCharge;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string ToString()
        {
            string query = PointRead ? "point read" : QueryText ?? "(none)";
            return Operation + " " + Container + ": " + query
                + (Parameters != null ? " " + Parameters : string.Empty)
                + (PartitionKey != null ? " [partition " + PartitionKey + "]" : string.Empty)
                + (ClientSide != null ? " | client: " + ClientSide : " | exact")
                + (DocumentsRead >= 0 ? " | read " + DocumentsRead.ToString(CultureInfo.InvariantCulture) : string.Empty)
                + " | " + RequestCharge.ToString("0.##", CultureInfo.InvariantCulture) + " RU";
        }

        #endregion
    }
}
