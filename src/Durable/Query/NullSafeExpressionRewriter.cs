namespace Durable.Query
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;

    /// <summary>
    /// Rewrites a lambda evaluated on the client so that member access, instance method calls, <c>Nullable.Value</c> and
    /// <see cref="Enumerable"/> calls on a null value produce the result type's default instead of throwing, the way a
    /// database propagates NULL (for example <c>x.Owner.Name</c> with no owner, or <c>x.Name.ToUpper()</c> with a null name).
    /// Sub-expressions that do not depend on a lambda parameter (captured variables) are left unchanged.
    /// Thread safety: not thread-safe; create one per rewrite.
    /// </summary>
    internal sealed class NullSafeExpressionRewriter : ExpressionVisitor
    {
        #region Public-Methods

        /// <summary>
        /// Rewrites and compiles a lambda.
        /// </summary>
        /// <typeparam name="TDelegate">Delegate type.</typeparam>
        /// <param name="lambda">Lambda. Must not be null.</param>
        /// <returns>The compiled, null-safe delegate.</returns>
        /// <exception cref="ArgumentNullException">Thrown when lambda is null.</exception>
        public static TDelegate Compile<TDelegate>(Expression<TDelegate> lambda) where TDelegate : Delegate
        {
            ArgumentNullException.ThrowIfNull(lambda);
            Expression<TDelegate> rewritten = (Expression<TDelegate>)new NullSafeExpressionRewriter().Visit(lambda);
            return rewritten.Compile();
        }

        /// <summary>
        /// Rewrites and compiles a single-parameter lambda whose result is boxed to <see cref="object"/>.
        /// </summary>
        /// <typeparam name="T">Parameter type.</typeparam>
        /// <param name="lambda">Lambda with one parameter of type <typeparamref name="T"/>. Must not be null.</param>
        /// <returns>The compiled, null-safe delegate.</returns>
        /// <exception cref="ArgumentNullException">Thrown when lambda is null.</exception>
        public static Func<T, object?> CompileBoxed<T>(LambdaExpression lambda)
        {
            ArgumentNullException.ThrowIfNull(lambda);
            LambdaExpression rewritten = (LambdaExpression)new NullSafeExpressionRewriter().Visit(lambda);
            Expression<Func<T, object?>> boxed = Expression.Lambda<Func<T, object?>>(Expression.Convert(rewritten.Body, typeof(object)), rewritten.Parameters);
            return boxed.Compile();
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override Expression VisitMember(MemberExpression node)
        {
            Expression? inner = node.Expression == null ? null : Visit(node.Expression);
            MemberExpression updated = node.Update(inner);
            if (inner == null || !MayBeNull(inner)) return updated;

            if (Nullable.GetUnderlyingType(inner.Type) != null)
            {
                if (node.Member.Name == "Value")
                    return Expression.Condition(Expression.Property(inner, "HasValue"), updated, Expression.Default(node.Type));
                return updated;
            }

            return Guard(inner, updated);
        }

        /// <inheritdoc />
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            Expression? instance = node.Object == null ? null : Visit(node.Object);
            Expression[] arguments = node.Arguments.Select(a => Visit(a)!).ToArray();
            MethodCallExpression updated = node.Update(instance, arguments);
            if (node.Method.ReturnType == typeof(void)) return updated;

            if (instance != null && MayBeNull(instance) && Nullable.GetUnderlyingType(instance.Type) == null)
                return Guard(instance, updated);

            if (instance == null && node.Method.DeclaringType == typeof(Enumerable) && arguments.Length > 0 && MayBeNull(arguments[0]))
                return Guard(arguments[0], updated);

            return updated;
        }

        private static Expression Guard(Expression value, Expression access)
        {
            return Expression.Condition(
                Expression.Equal(value, Expression.Constant(null, value.Type)),
                Expression.Default(access.Type),
                access);
        }

        private static bool MayBeNull(Expression expression)
        {
            if (expression.Type.IsValueType && Nullable.GetUnderlyingType(expression.Type) == null) return false;
            if (expression is ParameterExpression || expression is ConstantExpression) return false;
            return !ExpressionEvaluator.IsEvaluable(expression);
        }

        #endregion
    }
}
