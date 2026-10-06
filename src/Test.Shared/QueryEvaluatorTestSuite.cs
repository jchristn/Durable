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
    /// Unit tests for <see cref="QueryEvaluator{TRow}"/> through <see cref="DictionaryQueryEvaluator"/>, a subclass over
    /// plain dictionary rows: the client-side evaluator non-SQL backends reuse. No database is used.
    /// </summary>
    public class QueryEvaluatorTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// Filters follow C# null semantics and the node's string match mode.
        /// </summary>
        [Fact]
        public void FiltersFollowCSharpSemantics()
        {
            DictionaryQueryEvaluator evaluator = new DictionaryQueryEvaluator(Library());
            QuerySource source = Source<Author>();
            List<Dictionary<string, object?>> authors = Library()[typeof(Author)];

            Assert.Equal(new[] { 2, 3 }, Ids(evaluator.Filter(source, Normalize<Author>(source, x => x.CompanyId != 1), authors)));
            Assert.Equal(new[] { 1, 2 }, Ids(evaluator.Filter(source, Normalize<Author>(source, x => x.CompanyId > 0), authors)));
            Assert.Equal(new[] { 1 }, Ids(evaluator.Filter(source, Normalize<Author>(source, x => x.Name.StartsWith("a", StringComparison.OrdinalIgnoreCase)), authors)));
            Assert.Empty(evaluator.Filter(source, Normalize<Author>(source, x => x.Name.StartsWith("a", StringComparison.Ordinal)), authors));
            Assert.Equal(new[] { 3 }, Ids(evaluator.Filter(source, Normalize<Author>(source, x => x.Name.ToUpper() + "!" == "CY!"), authors)));
            Assert.Equal(3, evaluator.Filter(source, null, authors).Count);
        }

        /// <summary>
        /// Parameter values are converted with NormalizeValue to the stored form GetValue returns.
        /// </summary>
        [Fact]
        public void ParameterValuesAreNormalized()
        {
            Dictionary<string, object?> row = new Dictionary<string, object?> { ["Id"] = 1, ["Status"] = "Active" };
            QuerySource source = Source<ComplexEntity>();
            DictionaryQueryEvaluator evaluator = new DictionaryQueryEvaluator(new Dictionary<Type, List<Dictionary<string, object?>>>());
            evaluator.Bind(source, row);
            Assert.True(evaluator.Test(Normalize<ComplexEntity>(source, x => x.Status == Status.Active)));
            Assert.True(evaluator.Test(Normalize<ComplexEntity>(source, x => new[] { Status.Inactive, Status.Active }.Contains(x.Status))));
            Assert.False(evaluator.Test(Normalize<ComplexEntity>(source, x => x.Status == Status.Inactive)));
        }

        /// <summary>
        /// Navigation members, collections (including many-to-many) resolve through FindRows; soft-deleted rows are excluded.
        /// </summary>
        [Fact]
        public void NavigationsResolveThroughFindRows()
        {
            Dictionary<Type, List<Dictionary<string, object?>>> tables = Library();
            DictionaryQueryEvaluator evaluator = new DictionaryQueryEvaluator(tables);
            QuerySource books = Source<Book>();
            QuerySource authors = Source<Author>();

            Assert.Equal(new[] { 10, 11 }, Ids(evaluator.Filter(books, Normalize<Book>(books, x => x.Author!.Name == "Ann"), tables[typeof(Book)])));
            Assert.Equal(new[] { 1 }, Ids(evaluator.Filter(authors, Normalize<Author>(authors, x => x.Books.Count() == 2), tables[typeof(Author)])));
            Assert.Equal(new[] { 2, 3 }, Ids(evaluator.Filter(authors, Normalize<Author>(authors, x => !x.Books.Any(b => b.Title.StartsWith("A"))), tables[typeof(Author)])));
            Assert.Equal(new[] { 2 }, Ids(evaluator.Filter(authors, Normalize<Author>(authors, x => x.Categories.Any(c => c.Name == "SciFi")), tables[typeof(Author)])));

            QuerySource parents = Source<RelSoftParent>();
            Dictionary<string, object?> parent = new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "p" };
            tables[typeof(RelSoftNote)] = new List<Dictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["Id"] = 1, ["ParentId"] = 1, ["Text"] = "a", ["IsDeleted"] = false },
                new Dictionary<string, object?> { ["Id"] = 2, ["ParentId"] = 1, ["Text"] = "b", ["IsDeleted"] = true }
            };
            Assert.True(evaluator.Bind(parents, parent).Test(Normalize<RelSoftParent>(parents, x => x.Notes!.Count() == 1)));
            Assert.True(evaluator.IsSoftDeleted(EntityMetadata.For(typeof(RelSoftNote)), tables[typeof(RelSoftNote)][1]));
        }

        /// <summary>
        /// Apply filters, orders (stable, nulls first) and pages; Aggregate ignores nulls.
        /// </summary>
        [Fact]
        public void ApplyOrdersPagesAndAggregates()
        {
            Dictionary<Type, List<Dictionary<string, object?>>> tables = Library();
            DictionaryQueryEvaluator evaluator = new DictionaryQueryEvaluator(tables);
            QuerySource source = Source<Author>();
            EntityMetadata metadata = source.Metadata;
            ColumnMetadata company = metadata.Columns.Single(c => c.Property.Name == nameof(Author.CompanyId));
            ColumnMetadata id = metadata.Columns.Single(c => c.Property.Name == nameof(Author.Id));
            Expression<Func<Author, int?>> companySelector = x => x.CompanyId;
            Expression<Func<Author, int>> idSelector = x => x.Id;

            QueryModel model = new QueryModel(source);
            model.Orderings.Add(new QueryOrdering(new ColumnNode(source, company), companySelector, false));
            Assert.Equal(new[] { 3, 1, 2 }, Ids(evaluator.Apply(model, tables[typeof(Author)])));

            model.Orderings.Clear();
            model.Orderings.Add(new QueryOrdering(new ColumnNode(source, company), companySelector, true));
            model.Orderings.Add(new QueryOrdering(new ColumnNode(source, id), idSelector, true));
            model.Skip = 1;
            model.Take = 1;
            Assert.Equal(new[] { 1 }, Ids(evaluator.Apply(model, tables[typeof(Author)])));

            Assert.Equal(3m, evaluator.Aggregate(source, tables[typeof(Author)], AggregateFunction.Sum, new ColumnNode(source, company)));
            Assert.Equal(2, evaluator.Aggregate(source, tables[typeof(Author)], AggregateFunction.Max, new ColumnNode(source, company)));
        }

        #endregion

        #region Private-Methods

        private static Dictionary<Type, List<Dictionary<string, object?>>> Library()
        {
            return new Dictionary<Type, List<Dictionary<string, object?>>>
            {
                [typeof(Author)] = new List<Dictionary<string, object?>>
                {
                    new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ann", ["CompanyId"] = 1 },
                    new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bob", ["CompanyId"] = 2 },
                    new Dictionary<string, object?> { ["Id"] = 3, ["Name"] = "Cy", ["CompanyId"] = null }
                },
                [typeof(Book)] = new List<Dictionary<string, object?>>
                {
                    new Dictionary<string, object?> { ["Id"] = 10, ["Title"] = "Alpha", ["AuthorId"] = 1 },
                    new Dictionary<string, object?> { ["Id"] = 11, ["Title"] = "Beta", ["AuthorId"] = 1 },
                    new Dictionary<string, object?> { ["Id"] = 12, ["Title"] = "Gamma", ["AuthorId"] = 2 }
                },
                [typeof(Category)] = new List<Dictionary<string, object?>>
                {
                    new Dictionary<string, object?> { ["Id"] = 100, ["Name"] = "SciFi" },
                    new Dictionary<string, object?> { ["Id"] = 101, ["Name"] = "Drama" }
                },
                [typeof(AuthorCategory)] = new List<Dictionary<string, object?>>
                {
                    new Dictionary<string, object?> { ["Id"] = 1, ["AuthorId"] = 2, ["CategoryId"] = 100 },
                    new Dictionary<string, object?> { ["Id"] = 2, ["AuthorId"] = 1, ["CategoryId"] = 101 }
                }
            };
        }

        private static QuerySource Source<T>()
        {
            return new QuerySource(EntityMetadata.For(typeof(T)), "x");
        }

        private static QueryNode Normalize<T>(QuerySource source, Expression<Func<T, bool>> predicate)
        {
            return new QueryNormalizer().NormalizeLambda(predicate, source);
        }

        private static int[] Ids(IEnumerable<Dictionary<string, object?>> rows)
        {
            return rows.Select(row => (int)row["Id"]!).ToArray();
        }

        #endregion
    }
}
