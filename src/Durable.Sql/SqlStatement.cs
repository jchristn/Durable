namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text;

    /// <summary>
    /// A SQL command text with its bound parameters.
    /// Thread safety: immutable after construction; safe to share.
    /// </summary>
    public sealed class SqlStatement
    {
        #region Public-Members

        /// <summary>
        /// Gets the command text. Never null.
        /// </summary>
        public string Sql { get; }

        /// <summary>
        /// Gets the parameters in binding order. Never null.
        /// </summary>
        public IReadOnlyList<SqlParameterValue> Parameters { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a statement.
        /// </summary>
        /// <param name="sql">Command text. Must not be null.</param>
        /// <param name="parameters">Parameters; null means none.</param>
        /// <exception cref="ArgumentNullException">Thrown when sql is null.</exception>
        public SqlStatement(string sql, IReadOnlyList<SqlParameterValue>? parameters = null)
        {
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            Parameters = parameters ?? Array.Empty<SqlParameterValue>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Renders the command text with parameter values substituted, for diagnostics only. Never execute the result.
        /// </summary>
        /// <returns>Readable SQL.</returns>
        public string ToDebugString()
        {
            if (Parameters.Count == 0) return Sql;
            StringBuilder sb = new StringBuilder(Sql);
            for (int i = Parameters.Count - 1; i >= 0; i--)
            {
                SqlParameterValue parameter = Parameters[i];
                sb.Replace(parameter.Name, FormatDebugValue(parameter.Value));
            }

            return sb.ToString();
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Sql;
        }

        #endregion

        #region Private-Methods

        private static string FormatDebugValue(object? value)
        {
            if (value == null || value == DBNull.Value) return "NULL";
            switch (value)
            {
                case string s:
                    return "'" + s.Replace("'", "''", StringComparison.Ordinal) + "'";
                case bool b:
                    return b ? "TRUE" : "FALSE";
                case DateTime dt:
                    return "'" + dt.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture) + "'";
                case DateTimeOffset dto:
                    return "'" + dto.ToString("yyyy-MM-dd HH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture) + "'";
                case byte[] bytes:
                    return "0x" + Convert.ToHexString(bytes);
                case IFormattable formattable:
                    return formattable.ToString(null, CultureInfo.InvariantCulture);
                default:
                    return "'" + value.ToString() + "'";
            }
        }

        #endregion
    }
}
