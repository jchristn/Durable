namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Evaluates client-side expression subtrees (constants, captured variables, static members, method calls on them)
    /// so their values can be bound as parameters.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public static class ExpressionEvaluator
    {
        #region Public-Methods

        /// <summary>
        /// Determines whether an expression can be evaluated on the client: it references no lambda parameter other than
        /// those declared by lambdas nested inside it.
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>True when the expression has no free parameters.</returns>
        public static bool IsEvaluable(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            if (expression is ConstantExpression) return true;
            if (expression is ParameterExpression) return false;
            FreeParameterFinder finder = new FreeParameterFinder();
            finder.Visit(expression);
            return !finder.Found;
        }

        /// <summary>
        /// Evaluates an expression without free parameters.
        /// </summary>
        /// <param name="expression">Expression. Must not be null.</param>
        /// <returns>The value; may be null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when expression is null.</exception>
        public static object? Evaluate(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);
            switch (expression)
            {
                case ConstantExpression constant:
                    return constant.Value;
                case MemberExpression member when member.Expression == null || IsSimpleChain(member.Expression):
                    {
                        object? target = member.Expression == null ? null : Evaluate(member.Expression);
                        if (member.Expression != null && target == null)
                            throw new NullReferenceException("Cannot read '" + member.Member.Name + "' of a null value in a query expression.");
                        if (member.Member is FieldInfo field) return field.GetValue(target);
                        if (member.Member is PropertyInfo property) return property.GetValue(target);
                        break;
                    }
                case UnaryExpression unary when (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked) && unary.Operand is ConstantExpression:
                    break;
            }

            Expression<Func<object?>> lambda = Expression.Lambda<Func<object?>>(Expression.Convert(expression, typeof(object)));
            return lambda.Compile(preferInterpretation: true)();
        }

        #endregion

        #region Private-Methods

        private static bool IsSimpleChain(Expression expression)
        {
            while (true)
            {
                if (expression is ConstantExpression) return true;
                if (expression is MemberExpression member)
                {
                    if (member.Expression == null) return true;
                    expression = member.Expression;
                    continue;
                }

                return false;
            }
        }

        #endregion
    }
}
