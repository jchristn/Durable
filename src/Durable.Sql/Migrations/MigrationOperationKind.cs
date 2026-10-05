namespace Durable.Sql
{
    /// <summary>
    /// Kind of a schema change produced by <see cref="SchemaDiffer"/>. Operations are applied in the order
    /// DropIndex, CreateTable, AddColumn, DropColumn, CreateIndex.
    /// </summary>
    public enum MigrationOperationKind
    {
        /// <summary>
        /// Drop a secondary index (destructive).
        /// </summary>
        DropIndex = 0,

        /// <summary>
        /// Create a missing table.
        /// </summary>
        CreateTable = 1,

        /// <summary>
        /// Add a missing column to an existing table.
        /// </summary>
        AddColumn = 2,

        /// <summary>
        /// Drop a column that is not mapped by the entity (destructive).
        /// </summary>
        DropColumn = 3,

        /// <summary>
        /// Create a missing secondary index.
        /// </summary>
        CreateIndex = 4
    }
}
