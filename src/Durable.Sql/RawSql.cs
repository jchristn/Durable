namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text;

    /// <summary>
    /// Helpers for user-supplied SQL fragments.
    /// Thread safety: stateless.
    /// </summary>
    public static class RawSql
    {
        /// <summary>
        /// Replaces <c>{0}</c>, <c>{1}</c>... placeholders with bound parameters. <c>{{</c> and <c>}}</c> produce literal braces;
        /// other braces are left untouched. Each index is bound once even if referenced several times.
        /// </summary>
        /// <param name="sql">SQL fragment. Must not be null.</param>
        /// <param name="values">Placeholder values; may be null.</param>
        /// <param name="builder">Statement builder receiving parameters. Must not be null.</param>
        /// <param name="converter">Converter for values. Must not be null.</param>
        /// <returns>SQL with placeholders replaced.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql, builder or converter is null.</exception>
        /// <exception cref="FormatException">Thrown when a placeholder index has no value.</exception>
        public static string BindPlaceholders(string sql, object?[]? values, SqlStatementBuilder builder, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(converter);
            if (values == null || values.Length == 0)
                return sql.Contains("{{", StringComparison.Ordinal) || sql.Contains("}}", StringComparison.Ordinal)
                    ? sql.Replace("{{", "{", StringComparison.Ordinal).Replace("}}", "}", StringComparison.Ordinal)
                    : sql;

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
                    if (close > i + 1 && int.TryParse(sql.AsSpan(i + 1, close - i - 1), NumberStyles.None, CultureInfo.InvariantCulture, out int index))
                    {
                        if (index >= values.Length)
                            throw new FormatException("Placeholder {" + index + "} has no corresponding value (" + values.Length + " supplied).");
                        if (!names.TryGetValue(index, out string? name))
                        {
                            object? value = values[index];
                            name = value is SqlParameterValue named
                                ? builder.AddParameter(named.Value)
                                : builder.AddParameter(converter.ConvertToDatabase(value, null));
                            names[index] = name;
                        }

                        sb.Append(name);
                        i = close + 1;
                        continue;
                    }
                }

                sb.Append(c);
                i++;
            }

            return sb.ToString();
        }

        /// <summary>
        /// Builds a statement for raw SQL using positional parameters named by the dialect (@p0, @p1...).
        /// <see cref="SqlParameterValue"/> arguments keep their own names.
        /// </summary>
        /// <param name="sql">SQL. Must not be null.</param>
        /// <param name="values">Values; may be null.</param>
        /// <param name="dialect">Dialect. Must not be null.</param>
        /// <param name="converter">Converter. Must not be null.</param>
        /// <returns>The statement.</returns>
        /// <exception cref="ArgumentNullException">Thrown when sql, dialect or converter is null.</exception>
        public static SqlStatement Positional(string sql, object?[]? values, ISqlDialect dialect, IDataTypeConverter converter)
        {
            ArgumentNullException.ThrowIfNull(sql);
            ArgumentNullException.ThrowIfNull(dialect);
            ArgumentNullException.ThrowIfNull(converter);
            List<SqlParameterValue> parameters = new List<SqlParameterValue>();
            if (values != null)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    object? value = values[i];
                    if (value is SqlParameterValue named)
                        parameters.Add(named);
                    else
                        parameters.Add(new SqlParameterValue(dialect.FormatParameterName(i), converter.ConvertToDatabase(value, null)));
                }
            }

            return new SqlStatement(sql, parameters);
        }
    }
}
