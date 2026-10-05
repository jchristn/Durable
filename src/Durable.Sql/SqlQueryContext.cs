namespace Durable.Sql
{
    using System;

    /// <summary>
    /// The execution services a query builder needs: command executor, converter and dialect.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class SqlQueryContext
    {
        /// <summary>
        /// Gets the command executor. Never null.
        /// </summary>
        public SqlCommandExecutor Executor { get; }

        /// <summary>
        /// Gets the data type converter. Never null.
        /// </summary>
        public IDataTypeConverter Converter { get; }

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        public ISqlDialect Dialect => Executor.Dialect;

        /// <summary>
        /// Gets the repository options. Never null.
        /// </summary>
        public SqlRepositoryOptions Options => Executor.Options;

        /// <summary>
        /// Instantiates a context.
        /// </summary>
        /// <param name="executor">Executor. Must not be null.</param>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public SqlQueryContext(SqlCommandExecutor executor, IDataTypeConverter converter)
        {
            Executor = executor ?? throw new ArgumentNullException(nameof(executor));
            Converter = converter ?? throw new ArgumentNullException(nameof(converter));
        }

        /// <summary>
        /// Creates an expression translator for a statement, using this context's converter and string matching option.
        /// </summary>
        /// <param name="builder">Statement builder. Must not be null.</param>
        /// <returns>The translator.</returns>
        /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
        public SqlExpressionTranslator CreateTranslator(SqlStatementBuilder builder)
        {
            ArgumentNullException.ThrowIfNull(builder);
            return new SqlExpressionTranslator(builder, Converter, Options.StringMatching);
        }
    }
}
