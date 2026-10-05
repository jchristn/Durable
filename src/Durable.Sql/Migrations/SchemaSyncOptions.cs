namespace Durable.Sql
{
    /// <summary>
    /// Options controlling schema comparison and synchronization (<see cref="SqlMigrator.SyncSchema(System.Collections.Generic.IEnumerable{System.Type}, SchemaSyncOptions?)"/>,
    /// <see cref="MigrationContext.EnsureSchema(SchemaSyncOptions, System.Type[])"/>).
    /// <para>
    /// Rule for NOT NULL columns added to existing tables (existing rows need a value):
    /// 1) a constant <see cref="DefaultValueAttribute"/> (StaticValue, Zero, EmptyString, EmptyGuid, True, False) becomes the column DEFAULT;
    /// 2) otherwise, when <see cref="UseClrDefaultsForNotNullColumns"/> is true and the property is numeric, bool or an enum
    /// (without a value converter), the CLR default (0, false, the enum's zero value) becomes the DEFAULT;
    /// 3) otherwise the column is not added and a <see cref="SchemaDifferenceKind.NotNullColumnWithoutDefault"/> difference is
    /// reported, unless <see cref="AddUnresolvableNotNullColumnsAsNullable"/> is true, in which case it is added as NULL
    /// with a warning (a later sync then reports the nullability difference).
    /// The rule never inspects data, so generated scripts are deterministic.
    /// </para>
    /// Thread safety: not thread-safe for mutation; do not change while an operation is using the instance.
    /// </summary>
    public class SchemaSyncOptions
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets whether destructive operations (dropping unmapped columns and indexes, re-creating changed indexes)
        /// are applied. When false they are returned as skipped. Default: false.
        /// </summary>
        public bool AllowDestructive { get; set; } = false;

        /// <summary>
        /// Gets or sets whether a NOT NULL numeric, bool or enum column added to an existing table defaults to the CLR
        /// default value when no constant <see cref="DefaultValueAttribute"/> is present. Default: true.
        /// </summary>
        public bool UseClrDefaultsForNotNullColumns { get; set; } = true;

        /// <summary>
        /// Gets or sets whether a NOT NULL column for which no default can be derived is added as nullable (with a warning)
        /// instead of being reported as a manual step. Default: false.
        /// </summary>
        public bool AddUnresolvableNotNullColumnsAsNullable { get; set; } = false;

        /// <summary>
        /// Gets or sets whether indexes present in the database but not declared by the entity are dropped (destructive;
        /// requires <see cref="AllowDestructive"/>). Default: true.
        /// </summary>
        public bool DropUnmappedIndexes { get; set; } = true;

        /// <summary>
        /// Gets or sets whether columns present in the database but not mapped by the entity are dropped (destructive;
        /// requires <see cref="AllowDestructive"/>). Default: true.
        /// </summary>
        public bool DropUnmappedColumns { get; set; } = true;

        /// <summary>
        /// Gets or sets whether a synchronization runs in one transaction on databases with transactional DDL, so that a
        /// failure leaves the schema unchanged. Default: true.
        /// </summary>
        public bool UseTransaction { get; set; } = true;

        #endregion
    }
}
