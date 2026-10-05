namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Accumulates SQL text and parameters for a single command. Parameter names are generated sequentially
    /// using the dialect's naming, so fragments rendered into the same builder never collide.
    /// Thread safety: not thread-safe.
    /// </summary>
    public sealed class SqlStatementBuilder
    {
        #region Public-Members

        /// <summary>
        /// Gets the dialect. Never null.
        /// </summary>
        public ISqlDialect Dialect { get; }

        /// <summary>
        /// Gets the SQL text accumulated so far. Never null.
        /// </summary>
        public StringBuilder Sql { get; } = new StringBuilder(256);

        /// <summary>
        /// Gets the parameters added so far. Never null.
        /// </summary>
        public IReadOnlyList<SqlParameterValue> Parameters => _Parameters;

        /// <summary>
        /// Gets the number of parameters added so far.
        /// </summary>
        public int ParameterCount => _Parameters.Count;

        #endregion

        #region Private-Members

        private readonly List<SqlParameterValue> _Parameters = new List<SqlParameterValue>();
        private int _AliasCounter = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a builder.
        /// </summary>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when dialect is null.</exception>
        public SqlStatementBuilder(ISqlDialect dialect)
        {
            Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Appends raw SQL text.
        /// </summary>
        /// <param name="sql">SQL text; null is ignored.</param>
        /// <returns>This builder.</returns>
        public SqlStatementBuilder Append(string? sql)
        {
            if (sql != null) Sql.Append(sql);
            return this;
        }

        /// <summary>
        /// Appends a quoted identifier.
        /// </summary>
        /// <param name="identifier">Identifier. Must not be null.</param>
        /// <returns>This builder.</returns>
        public SqlStatementBuilder AppendIdentifier(string identifier)
        {
            Sql.Append(Dialect.QuoteIdentifier(identifier));
            return this;
        }

        /// <summary>
        /// Adds a parameter whose value is already in database form, and returns its placeholder.
        /// </summary>
        /// <param name="databaseValue">Database value; null becomes <see cref="DBNull"/>.</param>
        /// <param name="column">Associated column for provider-specific typing; may be null.</param>
        /// <returns>The placeholder to embed in SQL.</returns>
        public string AddParameter(object? databaseValue, ColumnMetadata? column = null)
        {
            string name = Dialect.FormatParameterName(_Parameters.Count);
            _Parameters.Add(new SqlParameterValue(name, databaseValue ?? DBNull.Value, column));
            return name;
        }

        /// <summary>
        /// Adds a parameter and appends its placeholder.
        /// </summary>
        /// <param name="databaseValue">Database value; null becomes <see cref="DBNull"/>.</param>
        /// <param name="column">Associated column; may be null.</param>
        /// <returns>This builder.</returns>
        public SqlStatementBuilder AppendParameter(object? databaseValue, ColumnMetadata? column = null)
        {
            Sql.Append(AddParameter(databaseValue, column));
            return this;
        }

        /// <summary>
        /// Adds an explicitly named parameter (for example, a stored procedure argument) and returns its name.
        /// </summary>
        /// <param name="parameter">Parameter. Must not be null.</param>
        /// <returns>The parameter name.</returns>
        /// <exception cref="ArgumentNullException">Thrown when parameter is null.</exception>
        public string AddNamedParameter(SqlParameterValue parameter)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            _Parameters.Add(parameter);
            return parameter.Name;
        }

        /// <summary>
        /// Returns a new unique table alias for subqueries (for example, "s1").
        /// </summary>
        /// <param name="prefix">Alias prefix. Default: "s".</param>
        /// <returns>The alias.</returns>
        public string NextAlias(string prefix = "s")
        {
            _AliasCounter++;
            return prefix + _AliasCounter;
        }

        /// <summary>
        /// Builds the statement.
        /// </summary>
        /// <returns>The statement.</returns>
        public SqlStatement Build()
        {
            return new SqlStatement(Sql.ToString(), _Parameters.ToArray());
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Sql.ToString();
        }

        #endregion
    }
}
