namespace Durable.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Describes a GROUP BY: the key selector split into key parts, and HAVING predicates over the grouping.
    /// Translates grouping expressions (<c>g.Key</c>, <c>g.Key.Part</c>, <c>g.Count()</c>, <c>g.Sum(x =&gt; ...)</c>,
    /// <c>g.Average</c>, <c>g.Min</c>, <c>g.Max</c>) into SQL for a translator.
    /// Thread safety: not thread-safe.
    /// </summary>
    public sealed class GroupingSpecification
    {
        #region Public-Members

        /// <summary>
        /// Gets the key selector. Never null.
        /// </summary>
        public LambdaExpression KeySelector { get; }

        /// <summary>
        /// Gets the grouping type (<c>IGrouping&lt;TKey, T&gt;</c>). Never null.
        /// </summary>
        public Type GroupingType { get; }

        /// <summary>
        /// Gets the HAVING predicates. Never null.
        /// </summary>
        public List<LambdaExpression> Having { get; } = new List<LambdaExpression>();

        #endregion

        #region Private-Members

        private readonly List<KeyValuePair<string?, Expression>> _KeyParts = new List<KeyValuePair<string?, Expression>>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a grouping specification.
        /// </summary>
        /// <param name="keySelector">Key selector over the entity. Must not be null.</param>
        /// <param name="groupingType">Grouping type. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public GroupingSpecification(LambdaExpression keySelector, Type groupingType)
        {
            KeySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            GroupingType = groupingType ?? throw new ArgumentNullException(nameof(groupingType));

            Expression body = keySelector.Body;
            if (body is NewExpression newExpression && newExpression.Members != null)
            {
                for (int i = 0; i < newExpression.Arguments.Count; i++)
                    _KeyParts.Add(new KeyValuePair<string?, Expression>(newExpression.Members[i].Name, newExpression.Arguments[i]));
            }
            else if (body is MemberInitExpression memberInit)
            {
                foreach (MemberBinding binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                        _KeyParts.Add(new KeyValuePair<string?, Expression>(assignment.Member.Name, assignment.Expression));
                }
            }
            else
            {
                _KeyParts.Add(new KeyValuePair<string?, Expression>(null, body));
            }
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Translates the key parts for GROUP BY.
        /// </summary>
        /// <param name="translator">Translator. Must not be null.</param>
        /// <param name="source">Root source. Must not be null.</param>
        /// <returns>SQL for each key part.</returns>
        public List<string> KeySql(SqlExpressionTranslator translator, TableSource source)
        {
            ArgumentNullException.ThrowIfNull(translator);
            translator.Bind(KeySelector.Parameters[0], source);
            return _KeyParts.Select(part => translator.Value(part.Value)).ToList();
        }

        /// <summary>
        /// Installs grouping translation on a translator.
        /// </summary>
        /// <param name="translator">Translator. Must not be null.</param>
        /// <param name="source">Root source. Must not be null.</param>
        public void Install(SqlExpressionTranslator translator, TableSource source)
        {
            ArgumentNullException.ThrowIfNull(translator);
            ArgumentNullException.ThrowIfNull(source);
            translator.Bind(KeySelector.Parameters[0], source);
            translator.CustomTranslator = (expression, t) => TryTranslate(expression, t, source);
        }

        #endregion

        #region Private-Methods

        private bool IsGrouping(Expression? expression)
        {
            return expression is ParameterExpression parameter && parameter.Type == GroupingType;
        }

        private string? TryTranslate(Expression expression, SqlExpressionTranslator translator, TableSource source)
        {
            if (expression is MemberExpression member)
            {
                if (member.Member.Name == "Key" && IsGrouping(member.Expression))
                {
                    if (_KeyParts.Count != 1)
                        throw new NotSupportedException("A composite group key cannot be used as a single value; reference its members (g.Key.Name).");
                    return WithoutCustom(translator, t => t.Value(_KeyParts[0].Value));
                }

                if (member.Expression is MemberExpression keyAccess && keyAccess.Member.Name == "Key" && IsGrouping(keyAccess.Expression))
                {
                    foreach (KeyValuePair<string?, Expression> part in _KeyParts)
                    {
                        if (part.Key == member.Member.Name) return WithoutCustom(translator, t => t.Value(part.Value));
                    }

                    if (_KeyParts.Count == 1)
                        return null;
                    throw new NotSupportedException("Group key has no member '" + member.Member.Name + "'.");
                }

                return null;
            }

            if (expression is MethodCallExpression call && call.Method.DeclaringType == typeof(Enumerable) && call.Arguments.Count >= 1 && IsGrouping(call.Arguments[0]))
            {
                LambdaExpression? lambda = call.Arguments.Count > 1 ? StripQuote(call.Arguments[1]) as LambdaExpression : null;
                switch (call.Method.Name)
                {
                    case "Count":
                    case "LongCount":
                        if (lambda == null) return "COUNT(*)";
                        return WithoutCustom(translator, t =>
                        {
                            t.Bind(lambda.Parameters[0], source);
                            return "SUM(CASE WHEN " + t.Predicate(lambda.Body) + " THEN 1 ELSE 0 END)";
                        });
                    case "Sum":
                    case "Min":
                    case "Max":
                    case "Average":
                        if (lambda == null) throw new NotSupportedException(call.Method.Name + " over a group requires a selector.");
                        return WithoutCustom(translator, t =>
                        {
                            t.Bind(lambda.Parameters[0], source);
                            string value = t.Value(lambda.Body);
                            if (call.Method.Name == "Average") return "AVG(CAST(" + value + " AS DECIMAL(38, 10)))";
                            string function = call.Method.Name == "Sum" ? "SUM" : call.Method.Name.ToUpperInvariant();
                            return function == "SUM" ? "COALESCE(SUM(" + value + "), 0)" : function + "(" + value + ")";
                        });
                    case "Any":
                        if (lambda == null) return "(COUNT(*) > 0)";
                        return WithoutCustom(translator, t =>
                        {
                            t.Bind(lambda.Parameters[0], source);
                            return "(SUM(CASE WHEN " + t.Predicate(lambda.Body) + " THEN 1 ELSE 0 END) > 0)";
                        });
                }

                throw new NotSupportedException("Group method '" + call.Method.Name + "' is not supported in SQL.");
            }

            return null;
        }

        private static string WithoutCustom(SqlExpressionTranslator translator, Func<SqlExpressionTranslator, string> action)
        {
            Func<Expression, SqlExpressionTranslator, string?>? custom = translator.CustomTranslator;
            translator.CustomTranslator = null;
            try
            {
                return action(translator);
            }
            finally
            {
                translator.CustomTranslator = custom;
            }
        }

        private static Expression StripQuote(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote) expression = ((UnaryExpression)expression).Operand;
            return expression;
        }

        #endregion
    }
}
