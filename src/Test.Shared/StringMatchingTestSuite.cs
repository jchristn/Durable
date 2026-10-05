namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// <see cref="StringMatchMode"/> coverage: with <see cref="StringMatchMode.Ordinal"/> or
    /// <see cref="StringMatchMode.IgnoreCase"/> (as the repository default or through an explicit
    /// <see cref="StringComparison"/> argument), string predicates return the same rows on every database regardless of
    /// its collation. <see cref="StringMatchMode.Database"/> keeps each database's collation behavior.
    /// </summary>
    public class StringMatchingTestSuite
    {
        #region Private-Members

        private static readonly string?[] _Names = new string?[] { "Alpha", "alpha", "ALPHA", "Café", "Cafe", "café", "Zebra", "50%_off", "50 off", null };

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="StringMatchingTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public StringMatchingTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Ordinal equality and inequality are case- and accent-sensitive; inequality keeps C# null semantics.
        /// </summary>
        [Fact]
        public async Task OrdinalEqualityIsExact()
        {
            ISqlRepository<StrMatchItem> repository = await SeedAsync(StringMatchMode.Ordinal);
            AssertNames(repository, x => x.Name == "alpha", "alpha");
            AssertNames(repository, x => x.Name == "café", "café");
            AssertNames(repository, x => x.Name != "alpha", "Alpha", "ALPHA", "Café", "Cafe", "café", "Zebra", "50%_off", "50 off", null);
            AssertNames(repository, x => x.Name!.Equals("Cafe"), "Cafe");
        }

        /// <summary>
        /// Ordinal Contains/StartsWith/EndsWith are case- and accent-sensitive and treat LIKE wildcards literally.
        /// </summary>
        [Fact]
        public async Task OrdinalSubstringTestsAreExact()
        {
            ISqlRepository<StrMatchItem> repository = await SeedAsync(StringMatchMode.Ordinal);
            AssertNames(repository, x => x.Name!.Contains("lph"), "Alpha", "alpha");
            AssertNames(repository, x => x.Name!.StartsWith("A"), "Alpha", "ALPHA");
            AssertNames(repository, x => x.Name!.EndsWith("é"), "Café", "café");
            AssertNames(repository, x => x.Name!.Contains("afe"), "Cafe");
            AssertNames(repository, x => x.Name!.Contains("%_"), "50%_off");
            AssertNames(repository, x => x.Name!.StartsWith(""), "Alpha", "alpha", "ALPHA", "Café", "Cafe", "café", "Zebra", "50%_off", "50 off");
            AssertNames(repository, x => x.Name!.EndsWith("ZEBRA"));
        }

        /// <summary>
        /// Ordinal applies to collection Contains (IN), Replace, IndexOf and ordering comparisons.
        /// </summary>
        [Fact]
        public async Task OrdinalAppliesToInReplaceIndexOfAndOrdering()
        {
            ISqlRepository<StrMatchItem> repository = await SeedAsync(StringMatchMode.Ordinal);
            List<string> wanted = new List<string> { "alpha", "café" };
            AssertNames(repository, x => wanted.Contains(x.Name!), "alpha", "café");
            AssertNames(repository, x => !wanted.Contains(x.Name!), "Alpha", "ALPHA", "Café", "Cafe", "Zebra", "50%_off", "50 off", null);
            AssertNames(repository, x => x.Name!.Replace("a", "4") == "Alph4", "Alpha");
            AssertNames(repository, x => x.Name!.IndexOf("l") == 1, "Alpha", "alpha");
            AssertNames(repository, x => x.Name != null && string.Compare(x.Name, "a", StringComparison.Ordinal) < 0, "Alpha", "ALPHA", "Café", "Cafe", "Zebra", "50%_off", "50 off");
            AssertNames(repository, x => x.Name != null && x.Name.CompareTo("Zebra") > 0, "alpha", "café");
        }

        /// <summary>
        /// An explicit <see cref="StringComparison.Ordinal"/> argument is exact even when the repository default is Database.
        /// </summary>
        [Fact]
        public async Task ExplicitOrdinalOverridesDatabaseDefault()
        {
            ISqlRepository<StrMatchItem> repository = await SeedAsync(StringMatchMode.Database);
            AssertNames(repository, x => x.Name!.Contains("lph", StringComparison.Ordinal), "Alpha", "alpha");
            AssertNames(repository, x => x.Name!.StartsWith("C", StringComparison.Ordinal), "Café", "Cafe");
            AssertNames(repository, x => x.Name!.EndsWith("fe", StringComparison.Ordinal), "Cafe");
            AssertNames(repository, x => string.Equals(x.Name, "ALPHA", StringComparison.Ordinal), "ALPHA");
            AssertNames(repository, x => x.Name!.Equals("café", StringComparison.Ordinal), "café");
        }

        /// <summary>
        /// Ignore-case matching folds case but stays accent-sensitive, as <see cref="StringComparison.OrdinalIgnoreCase"/> does.
        /// </summary>
        [Fact]
        public async Task IgnoreCaseFoldsCaseButNotAccents()
        {
            ISqlRepository<StrMatchItem> repository = await SeedAsync(StringMatchMode.IgnoreCase);
            AssertNames(repository, x => x.Name == "alpha", "Alpha", "alpha", "ALPHA");
            AssertNames(repository, x => x.Name == "CAFE", "Cafe");
            AssertNames(repository, x => x.Name!.Contains("AF"), "Café", "Cafe", "café");
            AssertNames(repository, x => x.Name!.EndsWith("É"), "Café", "café");
            AssertNames(repository, x => x.Name!.StartsWith("zeb"), "Zebra");
            List<string> wanted = new List<string> { "ALPHA", "zebra" };
            AssertNames(repository, x => wanted.Contains(x.Name!), "Alpha", "alpha", "ALPHA", "Zebra");
            AssertNames(repository, x => x.Name != "ALPHA", "Café", "Cafe", "café", "Zebra", "50%_off", "50 off", null);
        }

        /// <summary>
        /// An explicit <see cref="StringComparison.OrdinalIgnoreCase"/> argument folds case without folding accents on every database.
        /// </summary>
        [Fact]
        public async Task ExplicitIgnoreCaseOverridesDatabaseDefault()
        {
            ISqlRepository<StrMatchItem> repository = await SeedAsync(StringMatchMode.Database);
            AssertNames(repository, x => x.Name!.Equals("ALPHA", StringComparison.OrdinalIgnoreCase), "Alpha", "alpha", "ALPHA");
            AssertNames(repository, x => x.Name!.Contains("cafe", StringComparison.OrdinalIgnoreCase), "Cafe");
            AssertNames(repository, x => x.Name!.StartsWith("CAF", StringComparison.InvariantCultureIgnoreCase), "Café", "Cafe", "café");
            AssertNames(repository, x => string.Equals(x.Name, "café", StringComparison.CurrentCultureIgnoreCase), "Café", "café");
        }

        /// <summary>
        /// The Database mode keeps each database's collation: exact on SQLite and PostgreSQL, case-insensitive on SQL
        /// Server, and case- and accent-insensitive on MySQL with their default collations.
        /// </summary>
        [Fact]
        public async Task DatabaseModeFollowsCollation()
        {
            ISqlRepository<StrMatchItem> repository = await SeedAsync(StringMatchMode.Database);
            RepositoryType type = repository.Dialect.RepositoryType;
            if (type == RepositoryType.Sqlite || type == RepositoryType.Postgres)
            {
                AssertNames(repository, x => x.Name == "alpha", "alpha");
                AssertNames(repository, x => x.Name == "cafe");
            }
            else if (type == RepositoryType.SqlServer)
            {
                AssertNames(repository, x => x.Name == "alpha", "Alpha", "alpha", "ALPHA");
                AssertNames(repository, x => x.Name == "cafe", "Cafe");
            }
            else
            {
                AssertNames(repository, x => x.Name == "alpha", "Alpha", "alpha", "ALPHA");
                AssertNames(repository, x => x.Name == "cafe", "Café", "Cafe", "café");
            }
        }

        /// <summary>
        /// The mode also applies to Query() builders, counts and existence checks.
        /// </summary>
        [Fact]
        public async Task ModeAppliesToQueryBuilderAndAggregates()
        {
            ISqlRepository<StrMatchItem> ordinal = await SeedAsync(StringMatchMode.Ordinal);
            Assert.Equal(1, await ordinal.Query().Where(x => x.Name == "ALPHA").CountAsync());
            Assert.Equal(1L, ordinal.Count(x => x.Name!.StartsWith("Z")));
            Assert.False(await ordinal.ExistsAsync(x => x.Name == "zebra"));

            ISqlRepository<StrMatchItem> ignoreCase = RelTestHelpers.CreateRepository<StrMatchItem>(_Provider, new SqlRepositoryOptions { StringMatching = StringMatchMode.IgnoreCase });
            Assert.Equal(3, await ignoreCase.Query().Where(x => x.Name == "ALPHA").CountAsync());
            Assert.True(await ignoreCase.ExistsAsync(x => x.Name == "zebra"));
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<StrMatchItem>> SeedAsync(StringMatchMode mode)
        {
            ISqlRepository<StrMatchItem> repository = RelTestHelpers.CreateRepository<StrMatchItem>(_Provider, new SqlRepositoryOptions { StringMatching = mode });
            await RelTestHelpers.RecreateTableAsync(repository);
            await repository.CreateManyAsync(_Names.Select(n => new StrMatchItem { Name = n }).ToList());
            return repository;
        }

        private static void AssertNames(ISqlRepository<StrMatchItem> repository, Expression<Func<StrMatchItem, bool>> predicate, params string?[] expected)
        {
            List<string?> actual = repository.ReadMany(predicate).Select(x => x.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();
            List<string?> wanted = expected.OrderBy(n => n, StringComparer.Ordinal).ToList();
            Assert.True(wanted.SequenceEqual(actual),
                "Predicate " + predicate + " on " + repository.Dialect.RepositoryType + ": expected [" + string.Join(", ", wanted.Select(Show)) + "] but got [" + string.Join(", ", actual.Select(Show)) + "]");
        }

        private static string Show(string? value)
        {
            return value ?? "null";
        }

        #endregion
    }
}
