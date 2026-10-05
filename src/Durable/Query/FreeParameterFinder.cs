namespace Durable.Query
{
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary>
    /// Detects parameters that are not declared by a lambda inside the visited expression.
    /// </summary>
    internal sealed class FreeParameterFinder : ExpressionVisitor
    {
        private readonly HashSet<ParameterExpression> _Declared = new HashSet<ParameterExpression>();

        public bool Found { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (Found) return node;
            return base.Visit(node);
        }

        protected override Expression VisitLambda<TDelegate>(Expression<TDelegate> node)
        {
            foreach (ParameterExpression parameter in node.Parameters) _Declared.Add(parameter);
            return base.VisitLambda(node);
        }

        protected override Expression VisitBlock(BlockExpression node)
        {
            foreach (ParameterExpression variable in node.Variables) _Declared.Add(variable);
            return base.VisitBlock(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (!_Declared.Contains(node)) Found = true;
            return node;
        }
    }
}
