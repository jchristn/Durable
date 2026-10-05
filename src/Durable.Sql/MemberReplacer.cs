namespace Durable.Sql
{
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Rewrites accesses to members of a projection's result parameter into the expressions those members were bound to,
    /// so predicates and orderings over a projection translate against the source table.
    /// </summary>
    internal sealed class MemberReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _Parameter;
        private readonly Dictionary<string, Expression> _Bindings;

        internal MemberReplacer(ParameterExpression parameter, Dictionary<string, Expression> bindings)
        {
            _Parameter = parameter;
            _Bindings = bindings;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == _Parameter && node.Member is PropertyInfo)
            {
                if (_Bindings.TryGetValue(node.Member.Name, out Expression? replacement))
                    return replacement.Type == node.Type ? replacement : Expression.Convert(replacement, node.Type);
                throw new System.NotSupportedException("Projection member '" + node.Member.Name + "' is not assigned in the Select expression.");
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _Parameter)
                throw new System.NotSupportedException("A projected row can only be used through its assigned members.");
            return node;
        }
    }
}
