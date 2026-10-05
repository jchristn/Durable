namespace Durable.Sql
{
    using System;
    using System.Data.Common;

    /// <summary>
    /// Describes a command passed to <see cref="ISqlCommandInterceptor"/> callbacks.
    /// Thread safety: belongs to a single command execution; do not retain.
    /// </summary>
    public sealed class SqlCommandContext
    {
        /// <summary>
        /// Gets the ADO.NET command. Never null.
        /// </summary>
        public DbCommand Command { get; }

        /// <summary>
        /// Gets the logical operation (for example, "SELECT", "INSERT", "UPDATE", "DELETE", "RAW"). Never null.
        /// </summary>
        public string Operation { get; }

        /// <summary>
        /// Gets the entity type of the repository issuing the command. Never null.
        /// </summary>
        public Type EntityType { get; }

        /// <summary>
        /// Gets the entity's table name. Never null.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Instantiates the context.
        /// </summary>
        /// <param name="command">Command. Must not be null.</param>
        /// <param name="operation">Operation name. Must not be null.</param>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="tableName">Table name. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public SqlCommandContext(DbCommand command, string operation, Type entityType, string tableName)
        {
            Command = command ?? throw new ArgumentNullException(nameof(command));
            Operation = operation ?? throw new ArgumentNullException(nameof(operation));
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        }
    }
}
