namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text;

    /// <summary>
    /// Builds a CASE expression for a query's select list. Conditions are translated from LINQ (or taken as raw SQL);
    /// results are bound as parameters.
    /// Thread safety: not thread-safe.
    /// </summary>
    /// <typeparam name="T">Entity type.</typeparam>
    public class SqlCaseExpressionBuilder<T> : ICaseExpressionBuilder<T> where T : class, new()
    {
        #region Private-Members

        private readonly SqlQueryBuilder<T> _Parent;
        private readonly List<KeyValuePair<Func<SqlExpressionTranslator, TableSource, string>, object?>> _Branches = new List<KeyValuePair<Func<SqlExpressionTranslator, TableSource, string>, object?>>();
        private object? _Else;
        private bool _HasElse;

        #endregion

        #region Constructors-and-Factories

        internal SqlCaseExpressionBuilder(SqlQueryBuilder<T> parent)
        {
            _Parent = parent;
        }

        #endregion

        #region Public-Methods

        /// <inheritdoc />
        public ICaseExpressionBuilder<T> When(Expression<Func<T, bool>> condition, object? result)
        {
            ArgumentNullException.ThrowIfNull(condition);
            _Branches.Add(new KeyValuePair<Func<SqlExpressionTranslator, TableSource, string>, object?>((translator, source) => translator.TranslatePredicate(condition, source), result));
            return this;
        }

        /// <inheritdoc />
        public ICaseExpressionBuilder<T> WhenRaw(string condition, object? result)
        {
            ArgumentNullException.ThrowIfNull(condition);
            _Branches.Add(new KeyValuePair<Func<SqlExpressionTranslator, TableSource, string>, object?>((translator, source) => "(" + condition + ")", result));
            return this;
        }

        /// <inheritdoc />
        public ICaseExpressionBuilder<T> Else(object? result)
        {
            _Else = result;
            _HasElse = true;
            return this;
        }

        /// <inheritdoc />
        public ISqlQueryBuilder<T> EndCase(string alias)
        {
            if (_Branches.Count == 0) throw new ArgumentException("A CASE expression needs at least one WHEN branch.", nameof(alias));
            string quotedAlias = _Parent.Context.Dialect.QuoteIdentifier(SqlIdentifierValidator.RequireIdentifier(alias, nameof(alias)));
            List<KeyValuePair<Func<SqlExpressionTranslator, TableSource, string>, object?>> branches = new List<KeyValuePair<Func<SqlExpressionTranslator, TableSource, string>, object?>>(_Branches);
            bool hasElse = _HasElse;
            object? elseValue = _Else;

            _Parent.AddSelectItem((translator, source) =>
            {
                StringBuilder sb = new StringBuilder("CASE");
                foreach (KeyValuePair<Func<SqlExpressionTranslator, TableSource, string>, object?> branch in branches)
                    sb.Append(" WHEN ").Append(branch.Key(translator, source)).Append(" THEN ").Append(translator.Parameter(branch.Value, null));
                if (hasElse) sb.Append(" ELSE ").Append(translator.Parameter(elseValue, null));
                sb.Append(" END AS ").Append(quotedAlias);
                return sb.ToString();
            });
            return _Parent;
        }

        #endregion
    }
}
