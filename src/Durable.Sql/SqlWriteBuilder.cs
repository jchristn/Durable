namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Shared helpers that render write statements.
    /// Thread safety: stateless.
    /// </summary>
    public static class SqlWriteBuilder
    {
        /// <summary>
        /// Appends a DELETE for the given conditions, or an UPDATE setting the soft-delete marker when the entity has one.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <param name="metadata">Entity metadata. Must not be null.</param>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <param name="conditions">Conditions combined with AND; empty affects all rows.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public static void AppendDelete(SqlStatementBuilder builder, EntityMetadata metadata, IDataTypeConverter converter, IReadOnlyList<string> conditions)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(metadata);
            ArgumentNullException.ThrowIfNull(converter);
            ArgumentNullException.ThrowIfNull(conditions);

            ColumnMetadata? softDelete = metadata.SoftDeleteColumn;
            if (softDelete != null)
            {
                builder.Append("UPDATE ").AppendIdentifier(metadata.TableName).Append(" SET ").AppendIdentifier(softDelete.Name).Append(" = ")
                    .AppendParameter(converter.ConvertToDatabase(SoftDeleteValue(softDelete), softDelete), softDelete);
            }
            else
            {
                builder.Append("DELETE FROM ").AppendIdentifier(metadata.TableName);
            }

            if (conditions.Count > 0) builder.Append(" WHERE ").Append(string.Join(" AND ", conditions));
        }

        /// <summary>
        /// Returns the value written to a soft-delete column when a row is deleted.
        /// </summary>
        /// <param name="column">Soft-delete column. Must not be null.</param>
        /// <returns>True for bool columns, otherwise the current UTC time.</returns>
        public static object SoftDeleteValue(ColumnMetadata column)
        {
            ArgumentNullException.ThrowIfNull(column);
            if (column.ClrType == typeof(bool)) return true;
            if (column.ClrType == typeof(DateTimeOffset)) return DateTimeOffset.UtcNow;
            return DateTime.UtcNow;
        }
    }
}
