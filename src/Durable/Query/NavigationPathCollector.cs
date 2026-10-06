namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq.Expressions;
    using Durable;

    /// <summary>
    /// Finds the navigation paths a client-evaluated lambda reads from an entity parameter (for example
    /// <c>x.Owner.Name</c> reads <c>Owner</c>, <c>x.Author.Company.Name</c> reads <c>Author.Company</c>, and
    /// <c>x.Books.Count</c> reads <c>Books</c>), so they can be loaded with includes before the lambda runs.
    /// Thread safety: not thread-safe; create one per analysis.
    /// </summary>
    /// <typeparam name="T">Entity type whose parameters are analyzed.</typeparam>
    internal sealed class NavigationPathCollector<[DynamicallyAccessedMembers(EntityMetadata.RequiredMemberTypes)] T> : ExpressionVisitor
    {
        #region Public-Members

        /// <summary>
        /// Gets the collected paths as navigation-access lambdas over the entity parameter. Never null.
        /// </summary>
        public List<LambdaExpression> Paths { get; } = new List<LambdaExpression>();

        #endregion

        #region Private-Members

        private readonly HashSet<string> _Seen = new HashSet<string>(StringComparer.Ordinal);

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a collector.
        /// </summary>
        public NavigationPathCollector()
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Collects the navigation paths of several lambdas.
        /// </summary>
        /// <param name="lambdas">Lambdas. Must not be null.</param>
        /// <returns>The paths. Never null.</returns>
        public static List<LambdaExpression> Collect(IEnumerable<LambdaExpression> lambdas)
        {
            ArgumentNullException.ThrowIfNull(lambdas);
            NavigationPathCollector<T> collector = new NavigationPathCollector<T>();
            foreach (LambdaExpression lambda in lambdas) collector.Visit(lambda);
            return collector.Paths;
        }

        #endregion

        #region Private-Methods

        /// <inheritdoc />
        protected override Expression VisitMember(MemberExpression node)
        {
            List<MemberExpression> chain = new List<MemberExpression>();
            Expression? current = node;
            while (current is MemberExpression member)
            {
                chain.Insert(0, member);
                current = member.Expression;
            }

            if (current is ParameterExpression parameter && parameter.Type == typeof(T))
            {
                EntityMetadata owner = EntityMetadata.For<T>();
                MemberExpression? deepest = null;
                List<string> names = new List<string>();
                foreach (MemberExpression link in chain)
                {
                    NavigationMetadata? navigation = owner.FindNavigation(link.Member.Name);
                    if (navigation == null) break;
                    deepest = link;
                    names.Add(link.Member.Name);
                    if (navigation.IsCollection) break;
                    owner = EntityMetadata.For(navigation.RelatedType);
                }

                if (deepest != null && _Seen.Add(string.Join(".", names)))
                    Paths.Add(Expression.Lambda<Func<T, object?>>(Expression.Convert(deepest, typeof(object)), parameter));
            }

            return base.VisitMember(node);
        }

        #endregion
    }
}
