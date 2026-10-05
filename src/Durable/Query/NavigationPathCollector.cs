namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Durable;

    /// <summary>
    /// Finds the navigation paths a client-evaluated lambda reads from an entity parameter (for example
    /// <c>x.Owner.Name</c> reads <c>Owner</c>, <c>x.Author.Company.Name</c> reads <c>Author.Company</c>, and
    /// <c>x.Books.Count</c> reads <c>Books</c>), so they can be loaded with includes before the lambda runs.
    /// Thread safety: not thread-safe; create one per analysis.
    /// </summary>
    internal sealed class NavigationPathCollector : ExpressionVisitor
    {
        #region Public-Members

        /// <summary>
        /// Gets the collected paths as navigation-access lambdas over the entity parameter. Never null.
        /// </summary>
        public List<LambdaExpression> Paths { get; } = new List<LambdaExpression>();

        #endregion

        #region Private-Members

        private readonly Type _EntityType;
        private readonly HashSet<string> _Seen = new HashSet<string>(StringComparer.Ordinal);

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a collector.
        /// </summary>
        /// <param name="entityType">Entity type whose parameters are analyzed. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when entityType is null.</exception>
        public NavigationPathCollector(Type entityType)
        {
            _EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Collects the navigation paths of several lambdas.
        /// </summary>
        /// <param name="entityType">Entity type. Must not be null.</param>
        /// <param name="lambdas">Lambdas. Must not be null.</param>
        /// <returns>The paths. Never null.</returns>
        public static List<LambdaExpression> Collect(Type entityType, IEnumerable<LambdaExpression> lambdas)
        {
            ArgumentNullException.ThrowIfNull(lambdas);
            NavigationPathCollector collector = new NavigationPathCollector(entityType);
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

            if (current is ParameterExpression parameter && parameter.Type == _EntityType)
            {
                EntityMetadata owner = EntityMetadata.For(_EntityType);
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
                    Paths.Add(Expression.Lambda(deepest, parameter));
            }

            return base.VisitMember(node);
        }

        #endregion
    }
}
