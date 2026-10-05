namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Backend-neutral description of a read, count, aggregate, update or delete over one entity type, handed to an
    /// <see cref="IRepositoryBackend"/>. <see cref="Filter"/> already combines the caller's predicates with the
    /// repository's query filters and the soft-delete condition, so backends only evaluate it. Includes, projections and
    /// grouping are handled above the backend by <see cref="QueryBuilder{T}"/>.
    /// Thread safety: not thread-safe while being built; treat as immutable once passed to a backend.
    /// </summary>
    public sealed class QueryModel
    {
        #region Public-Members

        /// <summary>
        /// Gets the entity metadata. Never null.
        /// </summary>
        public EntityMetadata Metadata { get; }

        /// <summary>
        /// Gets the source the filter and ordering keys refer to. Never null.
        /// </summary>
        public QuerySource Source { get; }

        /// <summary>
        /// Gets or sets the row condition; null selects every row.
        /// </summary>
        public QueryNode? Filter { get; set; }

        /// <summary>
        /// Gets the ordering keys, most significant first. Never null.
        /// </summary>
        public List<QueryOrdering> Orderings { get; } = new List<QueryOrdering>();

        /// <summary>
        /// Gets or sets the number of rows to skip; null for none. Minimum: 0.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
        public int? Skip
        {
            get => _Skip;
            set
            {
                if (value.HasValue && value.Value < 0) throw new ArgumentOutOfRangeException(nameof(value), "Skip cannot be negative.");
                _Skip = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of rows; null for unlimited. Minimum: 0.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
        public int? Take
        {
            get => _Take;
            set
            {
                if (value.HasValue && value.Value < 0) throw new ArgumentOutOfRangeException(nameof(value), "Take cannot be negative.");
                _Take = value;
            }
        }

        /// <summary>
        /// Gets or sets whether duplicate rows are removed.
        /// </summary>
        public bool Distinct { get; set; }

        /// <summary>
        /// Gets or sets the transaction to run in; null for none.
        /// </summary>
        public ITransaction? Transaction { get; set; }

        #endregion

        #region Private-Members

        private int? _Skip;
        private int? _Take;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a model selecting every row of an entity.
        /// </summary>
        /// <param name="source">Source (entity). Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when source is null.</exception>
        public QueryModel(QuerySource source)
        {
            Source = source ?? throw new ArgumentNullException(nameof(source));
            Metadata = source.Metadata;
        }

        #endregion
    }
}
