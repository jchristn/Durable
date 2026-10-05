namespace Durable.Sql
{
    using System;
    using System.Data;

    /// <summary>
    /// A named parameter value bound to a SQL command.
    /// Thread safety: immutable after construction except for <see cref="Direction"/>-dependent <see cref="Value"/> updates
    /// performed by the command executor for output parameters.
    /// </summary>
    public sealed class SqlParameterValue
    {
        #region Public-Members

        /// <summary>
        /// Gets the parameter name including its prefix (for example, "@p0"). Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets or sets the value. Null is sent as <see cref="DBNull"/>. For output parameters, holds the returned value after execution.
        /// </summary>
        public object? Value { get; set; }

        /// <summary>
        /// Gets the mapped column the value is bound to, when known; used for provider-specific parameter typing. May be null.
        /// </summary>
        public ColumnMetadata? Column { get; }

        /// <summary>
        /// Gets or sets an explicit database type, or null to infer from the value. Default: null.
        /// </summary>
        public DbType? DbType { get; set; }

        /// <summary>
        /// Gets or sets the parameter direction. Default: <see cref="ParameterDirection.Input"/>.
        /// </summary>
        public ParameterDirection Direction { get; set; } = ParameterDirection.Input;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a parameter value.
        /// </summary>
        /// <param name="name">Parameter name including prefix. Must not be null or whitespace.</param>
        /// <param name="value">Value; may be null.</param>
        /// <param name="column">Associated column; may be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when name is null or whitespace.</exception>
        public SqlParameterValue(string name, object? value, ColumnMetadata? column = null)
        {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
            Name = name;
            Value = value;
            Column = column;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public override string ToString()
        {
            return Name + "=" + (Value == null || Value == DBNull.Value ? "NULL" : Value.ToString());
        }

        #endregion
    }
}
