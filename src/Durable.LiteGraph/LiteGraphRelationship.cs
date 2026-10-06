namespace Durable.LiteGraph
{
    using System;
    using Durable;

    /// <summary>
    /// A foreign key a <see cref="LiteGraphBackend"/> maintains as edges: for every dependent row whose foreign key column
    /// holds the key of an existing principal row, there is exactly one edge from the dependent's node to the principal's
    /// node, labelled <see cref="Label"/>, with a GUID derived from the relationship and the dependent node (see
    /// <see cref="LiteGraphBackend.GetEdgeGuid"/>). Relationships come from <see cref="ForeignKeyAttribute"/>s and from
    /// reference, inverse and many-to-many navigations whose principal side is the principal's single-column primary key;
    /// a many-to-many junction entity is stored as nodes like any other entity, with one edge to each side.
    /// Thread safety: immutable; safe for concurrent use.
    /// </summary>
    public sealed class LiteGraphRelationship
    {
        #region Public-Members

        /// <summary>
        /// Gets the stable identifier of the relationship: <c>dependentTable.column-&gt;principalTable</c>. Never null.
        /// It is stored in each edge's data as <c>relationship</c>.
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// Gets the edge label and name: the dependent's reference navigation property name for the foreign key when one
        /// exists, otherwise the foreign key property name. Never null.
        /// </summary>
        public string Label { get; }

        /// <summary>
        /// Gets the dependent (referencing) entity metadata; edges start at its nodes. Never null.
        /// </summary>
        public EntityMetadata Dependent { get; }

        /// <summary>
        /// Gets the foreign key column of <see cref="Dependent"/>. Never null.
        /// </summary>
        public ColumnMetadata ForeignKey { get; }

        /// <summary>
        /// Gets the principal (referenced) entity metadata; edges end at its nodes. Never null.
        /// </summary>
        public EntityMetadata Principal { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a relationship.
        /// </summary>
        /// <param name="dependent">Dependent entity. Must not be null.</param>
        /// <param name="foreignKey">Foreign key column of the dependent. Must not be null.</param>
        /// <param name="principal">Principal entity with a single-column primary key. Must not be null.</param>
        /// <param name="label">Edge label. Must not be null or empty.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="ArgumentException">Thrown when label is empty.</exception>
        public LiteGraphRelationship(EntityMetadata dependent, ColumnMetadata foreignKey, EntityMetadata principal, string label)
        {
            Dependent = dependent ?? throw new ArgumentNullException(nameof(dependent));
            ForeignKey = foreignKey ?? throw new ArgumentNullException(nameof(foreignKey));
            Principal = principal ?? throw new ArgumentNullException(nameof(principal));
            ArgumentNullException.ThrowIfNull(label);
            if (label.Length == 0) throw new ArgumentException("Label cannot be empty.", nameof(label));
            Label = label;
            Id = dependent.TableName + "." + foreignKey.Name + "->" + principal.TableName;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string ToString()
        {
            return Id + " [" + Label + "]";
        }

        #endregion
    }
}
