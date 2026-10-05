namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary>
    /// Describes a grouping: the key selector split into key parts, and the predicates applied to each group (HAVING).
    /// <see cref="QueryNormalizer.UseGrouping"/> uses it to translate grouping expressions (<c>g.Key</c>, <c>g.Key.Part</c>,
    /// <c>g.Count()</c>, <c>g.Sum(x =&gt; ...)</c>, <c>g.Average</c>, <c>g.Min</c>, <c>g.Max</c>, <c>g.Any</c>) into
    /// <see cref="AggregateNode"/>s and key nodes.
    /// Thread safety: not thread-safe while <see cref="Having"/> is being modified.
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
        /// Gets the group predicates (HAVING), as lambdas over the grouping type. Never null.
        /// </summary>
        public List<LambdaExpression> Having { get; } = new List<LambdaExpression>();

        /// <summary>
        /// Gets the key parts: the member name (null for a single, unnamed key) and the expression over the entity
        /// parameter of <see cref="KeySelector"/>. Never null; at least one element.
        /// </summary>
        public IReadOnlyList<KeyValuePair<string?, Expression>> KeyParts => _KeyParts;

        #endregion

        #region Private-Members

        private readonly List<KeyValuePair<string?, Expression>> _KeyParts = new List<KeyValuePair<string?, Expression>>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a grouping specification.
        /// </summary>
        /// <param name="keySelector">Key selector over the entity. Must not be null. Anonymous-type and member-init bodies are split into parts.</param>
        /// <param name="groupingType">The <c>IGrouping&lt;TKey, T&gt;</c> type. Must not be null.</param>
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
        /// Returns whether an expression is the grouping parameter (<c>g</c>).
        /// </summary>
        /// <param name="expression">Expression; may be null.</param>
        /// <returns>True when the expression is a parameter of <see cref="GroupingType"/>.</returns>
        public bool IsGrouping(Expression? expression)
        {
            return expression is ParameterExpression parameter && parameter.Type == GroupingType;
        }

        #endregion
    }
}
