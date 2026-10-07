namespace Durable.MySql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.Sql;

    /// <summary>
    /// MariaDB dialect (MariaDB 10.11+; tested on 11.4 LTS). Differences from <see cref="MySqlDialect"/>:
    /// upserts use <c>VALUES(column)</c> (MariaDB has no row alias), empty-string tests use <c>CHAR_LENGTH</c> (MariaDB
    /// collations pad trailing spaces), ordinal matching uses the NO PAD binary collation <c>utf8mb4_nopad_bin</c>,
    /// LEAD/LAG defaults are emulated (MariaDB has no default argument), and JSON columns (a LONGTEXT alias) introspect as
    /// <c>longtext</c>.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public class MariaDbDialect : MySqlDialect
    {
        #region Public-Members

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static new MariaDbDialect Default { get; } = new MariaDbDialect();

        /// <inheritdoc />
        public override MySqlFlavor Flavor => MySqlFlavor.MariaDb;

        /// <inheritdoc />
        public override string DbSystemName => "mariadb";

        /// <summary>
        /// Gets false: MariaDB's LEAD and LAG take no default argument, so the engine emulates it.
        /// </summary>
        public override bool SupportsOffsetFunctionDefault => false;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the dialect.
        /// </summary>
        /// <param name="converter">Converter; null uses <see cref="MySqlDataTypeConverter"/>.</param>
        /// <param name="ordinalCollation">Binary collation for ordinal string matching; must be valid for the character set of
        /// the compared columns. Default: utf8mb4_nopad_bin (binary and NO PAD, so trailing spaces are significant as in
        /// .NET ordinal comparison).</param>
        /// <exception cref="ArgumentException">Thrown when ordinalCollation is not a simple collation name.</exception>
        public MariaDbDialect(IDataTypeConverter? converter = null, string ordinalCollation = "utf8mb4_nopad_bin") : base(converter, ordinalCollation)
        {
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        /// <remarks>MariaDB's PAD SPACE collations treat <c>'   '</c> as equal to <c>''</c>; the character length does not.</remarks>
        public override string IsEmptyString(string expression)
        {
            return "CHAR_LENGTH(" + expression + ") = 0";
        }

        /// <inheritdoc />
        /// <remarks>MariaDB does not support the <c>AS alias</c> row reference of MySQL 8.0.19+; <c>VALUES(column)</c> is used instead.</remarks>
        public override void AppendUpsert(SqlStatementBuilder builder, EntityMetadata metadata, IReadOnlyList<ColumnMetadata> insertColumns, IReadOnlyList<string> placeholders, IReadOnlyList<ColumnMetadata> conflictColumns, IReadOnlyList<ColumnMetadata> updateColumns, string? versionPlaceholder = null)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(insertColumns);
            ArgumentNullException.ThrowIfNull(placeholders);
            ArgumentNullException.ThrowIfNull(conflictColumns);
            ArgumentNullException.ThrowIfNull(updateColumns);
            builder.Append("INSERT INTO ").AppendIdentifier(metadata.TableName).Append(" (")
                .Append(string.Join(", ", insertColumns.Select(c => QuoteIdentifier(c.Name))))
                .Append(") VALUES (").Append(string.Join(", ", placeholders)).Append(") ON DUPLICATE KEY UPDATE ");
            IReadOnlyList<ColumnMetadata> assigned = updateColumns.Count > 0 ? updateColumns : conflictColumns;
            builder.Append(string.Join(", ", assigned.Select(c => QuoteIdentifier(c.Name) + " = " + (c.IsVersion && versionPlaceholder != null ? versionPlaceholder : "VALUES(" + QuoteIdentifier(c.Name) + ")"))));
        }

        /// <inheritdoc />
        /// <remarks>MariaDB's JSON type is an alias of LONGTEXT (with a JSON_VALID check) and is reported as <c>longtext</c>.</remarks>
        public override string NormalizeColumnType(string columnType)
        {
            string type = base.NormalizeColumnType(columnType);
            return type == "json" ? "longtext" : type;
        }

        #endregion
    }
}
