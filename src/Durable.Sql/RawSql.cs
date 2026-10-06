namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// The one placeholder convention for user-supplied SQL in Durable (raw-SQL repository methods, procedures' SQL
    /// fragments, <c>ISqlQueryBuilder.WhereRaw</c>/<c>WhereInRaw</c>, <c>MigrationContext.ExecuteSqlRaw</c>):
    /// <list type="bullet">
    /// <item><description><c>{0}</c>, <c>{1}</c>... are replaced by bound parameters for the corresponding values, never by
    /// the values' text. An index may be referenced several times and is bound once.</description></item>
    /// <item><description><c>{{</c> and <c>}}</c> produce literal braces, as in <see cref="string.Format(string, object?[])"/>.</description></item>
    /// <item><description>When no parameters are supplied (null or empty), the text is used verbatim: nothing is parsed or
    /// unescaped, so parameterless DDL and JSON literals are never altered.</description></item>
    /// <item><description>A <see cref="SqlParameterValue"/> value keeps its own name, direction and type; its placeholder is
    /// replaced by that name.</description></item>
    /// </list>
    /// The <see cref="FormattableString"/> overloads (<c>FromSql($"... {value}")</c>, <c>ExecuteSql</c>, ...) use the same
    /// rules: every interpolation hole becomes a parameter, so they are safe by construction. Holes can therefore not
    /// supply identifiers or SQL text; use the <c>*Raw</c> methods with string concatenation for dynamic identifiers.
    /// Thread safety: stateless.
    /// </summary>
    public static class RawSql
    {
        /// <summary>
        /// Replaces <c>{0}</c>, <c>{1}</c>... placeholders with bound parameters added to <paramref name="builder"/>.
        /// When <paramref name="values"/> is null or empty the SQL is returned unchanged.
        /// </summary>
        /// <param name="sql">SQL fragment. Must not be null.</param>
        /// <param name="values">Placeholder values; may be null.</param>
        /// <param name="builder">Statement builder receiving parameters. Must not be null.</param>
        /// <param name="converter">Converter for values. Must not be null.</param>
        /// <returns>SQL with placeholders replaced.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql, builder or converter is null.</exception>
        /// <exception cref="FormatException">Thrown when a placeholder index has no value.</exception>
        public static string BindPlaceholders(string sql, IReadOnlyList<object?>? values, SqlStatementBuilder builder, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(converter);
            if (values == null || values.Count == 0) return sql;
            return Bind(sql, values, builder, converter, false);
        }

        /// <summary>
        /// Builds a statement from SQL text and values using the placeholder convention described on <see cref="RawSql"/>.
        /// </summary>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="parameters">Placeholder values; null or empty sends the SQL verbatim.</param>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql, dialect or converter is null.</exception>
        /// <exception cref="FormatException">Thrown when a placeholder index has no value.</exception>
        public static SqlStatement ToStatement(string sql, IEnumerable<object?>? parameters, ISqlDialect dialect, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(dialect);
            ArgumentNullException.ThrowIfNull(converter);
            IReadOnlyList<object?>? values = parameters == null ? null : parameters as IReadOnlyList<object?> ?? parameters.ToList();
            if (values == null || values.Count == 0) return new SqlStatement(sql, new List<SqlParameterValue>());
            SqlStatementBuilder builder = new SqlStatementBuilder(dialect);
            builder.Append(Bind(sql, values, builder, converter, false));
            return builder.Build();
        }

        /// <summary>
        /// Builds a statement from an interpolated string: every hole becomes a bound parameter and <c>{{</c>/<c>}}</c> are
        /// literal braces.
        /// </summary>
        /// <param name="sql">Interpolated SQL. Must not be null.</param>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql, dialect or converter is null.</exception>
        /// <exception cref="FormatException">Thrown when a hole uses an alignment or format specifier (format the value first).</exception>
        public static SqlStatement ToStatement(FormattableString sql, ISqlDialect dialect, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(dialect);
            ArgumentNullException.ThrowIfNull(converter);
            SqlStatementBuilder builder = new SqlStatementBuilder(dialect);
            builder.Append(Bind(sql.Format, sql.GetArguments(), builder, converter, true));
            return builder.Build();
        }

        internal static string BindInterpolated(FormattableString sql, SqlStatementBuilder builder, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(sql);
            return Bind(sql.Format, sql.GetArguments(), builder, converter, true);
        }

        private static string Bind(string sql, IReadOnlyList<object?> values, SqlStatementBuilder builder, IDataTypeConverter converter, bool interpolated)
        {
            Dictionary<int, string> names = new Dictionary<int, string>();
            StringBuilder sb = new StringBuilder(sql.Length + 16);
            int i = 0;
            while (i < sql.Length)
            {
                char c = sql[i];
                if (c == '{' && i + 1 < sql.Length && sql[i + 1] == '{')
                {
                    sb.Append('{');
                    i += 2;
                    continue;
                }

                if (c == '}' && i + 1 < sql.Length && sql[i + 1] == '}')
                {
                    sb.Append('}');
                    i += 2;
                    continue;
                }

                if (c == '{')
                {
                    int close = sql.IndexOf('}', i + 1);
                    if (close > i + 1)
                    {
                        ReadOnlySpan<char> inner = sql.AsSpan(i + 1, close - i - 1);
                        if (int.TryParse(inner, NumberStyles.None, CultureInfo.InvariantCulture, out int index))
                        {
                            if (index >= values.Count)
                                throw new FormatException("Placeholder {" + index + "} has no corresponding value (" + values.Count + " supplied).");
                            if (!names.TryGetValue(index, out string? name))
                            {
                                object? value = values[index];
                                name = value is SqlParameterValue named
                                    ? builder.AddNamedParameter(named)
                                    : builder.AddParameter(converter.ConvertToDatabase(value, null));
                                names[index] = name;
                            }

                            sb.Append(name);
                            i = close + 1;
                            continue;
                        }

                        if (interpolated)
                        {
                            int separator = inner.IndexOfAny(',', ':');
                            if (separator > 0 && int.TryParse(inner.Slice(0, separator), NumberStyles.None, CultureInfo.InvariantCulture, out _))
                                throw new FormatException("Alignment and format specifiers are not supported in SQL interpolation holes ({" + inner.ToString() + "}); format the value before interpolating it.");
                        }
                    }
                }

                sb.Append(c);
                i++;
            }

            return sb.ToString();
        }
    }
}
