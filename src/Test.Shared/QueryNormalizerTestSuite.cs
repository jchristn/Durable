namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Durable;
    using Durable.Query;
    using Xunit;

    /// <summary>
    /// Unit tests for <see cref="QueryNormalizer"/>: the backend-neutral node tree every backend translates. No database is used.
    /// </summary>
    public class QueryNormalizerTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// Captured variables are evaluated into value nodes; the column side keeps its position.
        /// </summary>
        [Fact]
        public void CapturedValuesBecomeValueNodes()
        {
            int threshold = 5;
            ComparisonNode node = Assert.IsType<ComparisonNode>(Normalize<ComplexEntity>(x => x.NullableInt > threshold));
            Assert.Equal(ComparisonOperator.GreaterThan, node.Operator);
            ColumnNode column = Assert.IsType<ColumnNode>(node.Left);
            Assert.Equal("nullable_int", column.Column.Name);
            ValueNode value = Assert.IsType<ValueNode>(node.Right);
            Assert.Equal(5, value.Value);
            Assert.Same(column.Column, value.Column);

            ComparisonNode flipped = Assert.IsType<ComparisonNode>(Normalize<ComplexEntity>(x => threshold < x.NullableInt));
            Assert.IsType<ValueNode>(flipped.Left);
            Assert.IsType<ColumnNode>(flipped.Right);
        }

        /// <summary>
        /// Comparisons with null become null checks; HasValue becomes a not-null check.
        /// </summary>
        [Fact]
        public void NullComparisonsBecomeNullChecks()
        {
            NullCheckNode isNull = Assert.IsType<NullCheckNode>(Normalize<ComplexEntity>(x => x.NullableInt == null));
            Assert.True(isNull.IsNull);
            NullCheckNode notNull = Assert.IsType<NullCheckNode>(Normalize<ComplexEntity>(x => null != x.NullableInt));
            Assert.False(notNull.IsNull);
            NullCheckNode hasValue = Assert.IsType<NullCheckNode>(Normalize<ComplexEntity>(x => x.NullableInt.HasValue));
            Assert.False(hasValue.IsNull);
        }

        /// <summary>
        /// Integers compared with an enum column are converted to the enum, so backends store them by the column's rules.
        /// </summary>
        [Fact]
        public void IntegersComparedWithEnumColumnsBecomeEnums()
        {
            ComparisonNode node = Assert.IsType<ComparisonNode>(Normalize<ComplexEntity>(x => (int)x.Status == 1));
            ValueNode value = Assert.IsType<ValueNode>(node.Right);
            Assert.Equal(Status.Inactive, value.Value);

            InNode membership = Assert.IsType<InNode>(Normalize<ComplexEntity>(x => new[] { Status.Active }.Contains(x.Status)));
            Assert.Equal(Status.Active, Assert.Single(membership.Values));
        }

        /// <summary>
        /// Explicit StringComparison arguments set the mode; otherwise the normalizer's default applies.
        /// </summary>
        [Fact]
        public void StringComparisonSelectsMode()
        {
            StringMatchNode ordinal = Assert.IsType<StringMatchNode>(Normalize<Author>(x => x.Name.StartsWith("A", StringComparison.Ordinal)));
            Assert.Equal(StringMatchMode.Ordinal, ordinal.Mode);
            Assert.Equal(StringMatchKind.StartsWith, ordinal.Kind);

            StringMatchNode ignoreCase = Assert.IsType<StringMatchNode>(Normalize<Author>(x => x.Name.Contains("a", StringComparison.InvariantCultureIgnoreCase)));
            Assert.Equal(StringMatchMode.IgnoreCase, ignoreCase.Mode);

            StringMatchNode culture = Assert.IsType<StringMatchNode>(Normalize<Author>(x => x.Name.EndsWith("a", StringComparison.CurrentCulture), StringMatchMode.Ordinal));
            Assert.Equal(StringMatchMode.Database, culture.Mode);

            ComparisonNode defaulted = Assert.IsType<ComparisonNode>(Normalize<Author>(x => x.Name == "a", StringMatchMode.IgnoreCase));
            Assert.Equal(StringMatchMode.IgnoreCase, defaulted.StringMode);

            ComparisonNode equals = Assert.IsType<ComparisonNode>(Normalize<Author>(x => string.Equals(x.Name, "a", StringComparison.OrdinalIgnoreCase)));
            Assert.Equal(StringMatchMode.IgnoreCase, equals.StringMode);

            ComparisonNode compare = Assert.IsType<ComparisonNode>(Normalize<Author>(x => string.CompareOrdinal(x.Name, "m") < 0));
            Assert.Equal(ComparisonOperator.LessThan, compare.Operator);
            Assert.Equal(StringMatchMode.Ordinal, compare.StringMode);
        }

        /// <summary>
        /// Array, list and span-based Contains all become IN nodes; empty lists are preserved for the backend to decide.
        /// </summary>
        [Fact]
        public void CollectionContainsBecomesInNode()
        {
            int[] ids = new[] { 1, 2, 2, 3 };
            List<int> list = new List<int> { 4 };
            InNode array = Assert.IsType<InNode>(Normalize<Author>(x => ids.Contains(x.Id)));
            Assert.Equal(4, array.Values.Count);
            Assert.False(array.Negated);
            Assert.IsType<InNode>(Normalize<Author>(x => list.Contains(x.Id)));
            InNode helper = Assert.IsType<InNode>(Normalize<Author>(x => x.Id.NotIn(1, 2)));
            Assert.True(helper.Negated);
            InNode empty = Assert.IsType<InNode>(Normalize<Author>(x => Array.Empty<int>().Contains(x.Id)));
            Assert.Empty(empty.Values);
        }

        /// <summary>
        /// Navigation members and collection operations become navigation nodes with their own sources.
        /// </summary>
        [Fact]
        public void NavigationsBecomeNavigationNodes()
        {
            ComparisonNode member = Assert.IsType<ComparisonNode>(Normalize<Book>(x => x.Author!.Name == "Ann"));
            NavigationMemberNode navigation = Assert.IsType<NavigationMemberNode>(member.Left);
            Assert.Equal("Author", navigation.Navigation.Name);
            Assert.Equal("name", navigation.Column.Name);
            Assert.IsType<ColumnNode>(navigation.OwnerKey);
            Assert.Equal(typeof(Author), navigation.RelatedSource.Metadata.EntityType);

            CollectionNode any = Assert.IsType<CollectionNode>(Normalize<Author>(x => x.Books.Any(b => b.Title.StartsWith("C"))));
            Assert.Equal(CollectionOperation.Any, any.Operation);
            StringMatchNode inner = Assert.IsType<StringMatchNode>(any.Predicate);
            Assert.Same(any.RelatedSource, Assert.IsType<ColumnNode>(inner.Target).Source);

            ComparisonNode count = Assert.IsType<ComparisonNode>(Normalize<Author>(x => x.Books.Count() > 2));
            Assert.Equal(CollectionOperation.Count, Assert.IsType<CollectionNode>(count.Left).Operation);
        }

        /// <summary>
        /// Functions, concatenation and integer division carry the information backends need.
        /// </summary>
        [Fact]
        public void FunctionsAndArithmeticKeepSemantics()
        {
            ComparisonNode length = Assert.IsType<ComparisonNode>(Normalize<Author>(x => x.Name.Trim().Length > 3));
            FunctionNode lengthFunction = Assert.IsType<FunctionNode>(length.Left);
            Assert.Equal(QueryFunction.Length, lengthFunction.Function);
            Assert.Equal(QueryFunction.Trim, Assert.IsType<FunctionNode>(lengthFunction.Arguments[0]).Function);

            ComparisonNode concat = Assert.IsType<ComparisonNode>(Normalize<Author>(x => x.Name + "-" + x.Id == "a-1"));
            ConcatNode parts = Assert.IsType<ConcatNode>(concat.Left);
            Assert.Equal(3, parts.Parts.Count);
            Assert.Equal(typeof(int), parts.Parts[2].ClrType);

            ComparisonNode integer = Assert.IsType<ComparisonNode>(Normalize<Author>(x => x.Id / 2 == 1));
            Assert.True(Assert.IsType<ArithmeticNode>(integer.Left).IntegerDivision);
            ComparisonNode real = Assert.IsType<ComparisonNode>(Normalize<Author>(x => (double)x.Id / 2 == 0.5));
            Assert.False(Assert.IsType<ArithmeticNode>(real.Left).IntegerDivision);
        }

        /// <summary>
        /// Grouping expressions become key nodes and aggregates over the grouped rows.
        /// </summary>
        [Fact]
        public void GroupingBecomesAggregates()
        {
            Expression<Func<Book, int>> key = b => b.AuthorId;
            GroupingSpecification grouping = new GroupingSpecification(key, typeof(IGrouping<int, Book>));
            QuerySource source = new QuerySource(EntityMetadata.For(typeof(Book)));
            QueryNormalizer normalizer = new QueryNormalizer().UseGrouping(grouping, source);

            Expression<Func<IGrouping<int, Book>, bool>> having = g => g.Count(b => b.PublisherId != null) > 1 && g.Key > 0;
            LogicalNode logical = Assert.IsType<LogicalNode>(normalizer.NormalizeLambda(having, source));
            AggregateNode count = Assert.IsType<AggregateNode>(Assert.IsType<ComparisonNode>(logical.Left).Left);
            Assert.Equal(AggregateFunction.Count, count.Function);
            Assert.IsType<NullCheckNode>(count.Operand);
            ColumnNode keyColumn = Assert.IsType<ColumnNode>(Assert.IsType<ComparisonNode>(logical.Right).Left);
            Assert.Equal("author_id", keyColumn.Column.Name);

            List<QueryNode> keyParts = new QueryNormalizer().NormalizeGroupKey(grouping, source);
            Assert.Single(keyParts);
        }

        /// <summary>
        /// Fully client-side predicates fold to constants; untranslatable members fail with a clear message.
        /// </summary>
        [Fact]
        public void ConstantsFoldAndUnsupportedConstructsThrow()
        {
            bool flag = false;
            ValueNode constant = Assert.IsType<ValueNode>(Normalize<Author>(x => flag || 1 > 2));
            Assert.Equal(false, constant.Value);

            NotSupportedException method = Assert.Throws<NotSupportedException>(() => Normalize<Author>(x => x.Name.GetHashCode() == 1));
            Assert.Contains("cannot be translated", method.Message);
            NotSupportedException navigation = Assert.Throws<NotSupportedException>(() => Normalize<Book>(x => x.Author == null));
            Assert.Contains("Navigation 'Author'", navigation.Message);
        }

        /// <summary>
        /// The capability validator rejects features a backend lacks, naming the capability.
        /// </summary>
        [Fact]
        public void CapabilityValidatorNamesMissingCapability()
        {
            QueryNode navigation = Normalize<Book>(x => x.Author!.Name == "Ann");
            NotSupportedException error = Assert.Throws<NotSupportedException>(() => QueryCapabilityValidator.Validate(navigation, RepositoryCapabilities.All & ~RepositoryCapabilities.NavigationPredicates));
            Assert.Contains("NavigationPredicates", error.Message);

            QueryNode function = Normalize<Author>(x => x.Name.ToUpper() == "A");
            Assert.Throws<NotSupportedException>(() => QueryCapabilityValidator.Validate(function, RepositoryCapabilities.None));
            QueryCapabilityValidator.Validate(Normalize<Author>(x => x.Name == "A" && x.Id > 1), RepositoryCapabilities.None);

            QueryNode ordinal = Normalize<Author>(x => x.Name == "A", StringMatchMode.Ordinal);
            Assert.Contains("StringMatchModes", Assert.Throws<NotSupportedException>(() => QueryCapabilityValidator.Validate(ordinal, RepositoryCapabilities.None)).Message);
        }

        #endregion

        #region Private-Methods

        private static QueryNode Normalize<T>(Expression<Func<T, bool>> predicate, StringMatchMode mode = StringMatchMode.Database)
        {
            QueryNormalizer normalizer = new QueryNormalizer(mode);
            return normalizer.NormalizeLambda(predicate, new QuerySource(EntityMetadata.For(typeof(T)), "x"));
        }

        #endregion
    }
}
