namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// String matching modes. <see cref="StringMatchMode.Database"/> results are backend-defined (collations), so only
    /// collation-independent facts are asserted there; <see cref="StringMatchMode.Ordinal"/> and
    /// <see cref="StringMatchMode.IgnoreCase"/> (repository option or explicit <see cref="StringComparison"/> argument)
    /// must behave exactly like C# (<see cref="RepositoryCapabilities.StringMatchModes"/>).
    /// Data: Alpha, alpha, ALPHA, Cafe, Café, café, Zebra, 50%_off and a null.
    /// </summary>
    internal sealed class StringMatchingSuite : KitSuite
    {
        private static readonly string?[] _Values = new string?[] { "Alpha", "alpha", "ALPHA", "Cafe", "Café", "café", "Zebra", "50%_off", null };

        public StringMatchingSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Database mode: an exact-case value is always found and only case or accent variants may accompany it")]
        public async Task DatabaseModeFindsExactValues()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Database);
            List<string?> alpha = repository.ReadMany(x => x.Text == "alpha").Select(x => x.Text).ToList();
            Assert.Contains("alpha", alpha);
            Assert.All(alpha, t => Assert.True(string.Equals(t, "alpha", StringComparison.OrdinalIgnoreCase), "Database-mode equality returned unrelated value " + t));
            AssertTexts(repository, x => x.Text == "Zebra", "Zebra");
            AssertTexts(repository, x => x.Text!.StartsWith("Ze"), "Zebra");
        }

        [ConformanceTest(Description = "Database mode: wildcard characters are literal and != includes nulls")]
        public async Task DatabaseModeLiteralWildcardsAndNulls()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Database);
            AssertTexts(repository, x => x.Text!.Contains("%_"), "50%_off");
            AssertTexts(repository, x => x.Text != "Zebra", "Alpha", "alpha", "ALPHA", "Cafe", "Café", "café", "50%_off", null);
            AssertTexts(repository, x => x.Text == null, new string?[] { null });
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes, Description = "Ordinal mode: equality is exact")]
        public async Task OrdinalEquality()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Ordinal);
            AssertTexts(repository, x => x.Text == "alpha", "alpha");
            AssertTexts(repository, x => x.Text == "café", "café");
            AssertTexts(repository, x => x.Text != "alpha", "Alpha", "ALPHA", "Cafe", "Café", "café", "Zebra", "50%_off", null);
            AssertTexts(repository, x => x.Text!.Equals("Cafe"), "Cafe");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes, Description = "Ordinal mode: Contains / StartsWith / EndsWith are exact")]
        public async Task OrdinalPatterns()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Ordinal);
            AssertTexts(repository, x => x.Text!.Contains("lph"), "Alpha", "alpha");
            AssertTexts(repository, x => x.Text!.StartsWith("A"), "Alpha", "ALPHA");
            AssertTexts(repository, x => x.Text!.EndsWith("é"), "Café", "café");
            AssertTexts(repository, x => x.Text!.Contains("afe"), "Cafe");
            AssertTexts(repository, x => x.Text!.Contains("%_"), "50%_off");
            AssertTexts(repository, x => x.Text!.EndsWith("ZEBRA"));
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes, Description = "Ordinal mode applies to IN lists and string comparisons")]
        public async Task OrdinalMembershipAndComparison()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Ordinal);
            List<string> wanted = new List<string> { "alpha", "café" };
            AssertTexts(repository, x => wanted.Contains(x.Text!), "alpha", "café");
            AssertTexts(repository, x => !wanted.Contains(x.Text!), "Alpha", "ALPHA", "Cafe", "Café", "Zebra", "50%_off", null);
            AssertTexts(repository, x => x.Text != null && string.Compare(x.Text, "a", StringComparison.Ordinal) < 0, "Alpha", "ALPHA", "Cafe", "Café", "Zebra", "50%_off");
            AssertTexts(repository, x => x.Text != null && x.Text.CompareTo("Zebra") > 0, "alpha", "café");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes | RepositoryCapabilities.Functions, Description = "Ordinal mode applies to Replace and IndexOf")]
        public async Task OrdinalFunctions()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Ordinal);
            AssertTexts(repository, x => x.Text!.Replace("a", "4") == "Alph4", "Alpha");
            AssertTexts(repository, x => x.Text!.IndexOf("l") == 1, "Alpha", "alpha");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes, Description = "Explicit StringComparison.Ordinal overrides the Database default")]
        public async Task ExplicitOrdinal()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Database);
            AssertTexts(repository, x => x.Text!.Contains("lph", StringComparison.Ordinal), "Alpha", "alpha");
            AssertTexts(repository, x => x.Text!.StartsWith("C", StringComparison.Ordinal), "Cafe", "Café");
            AssertTexts(repository, x => x.Text!.EndsWith("fe", StringComparison.Ordinal), "Cafe");
            AssertTexts(repository, x => string.Equals(x.Text, "ALPHA", StringComparison.Ordinal), "ALPHA");
            AssertTexts(repository, x => x.Text!.Equals("café", StringComparison.Ordinal), "café");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes, Description = "IgnoreCase mode folds case but not accents")]
        public async Task IgnoreCaseMode()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.IgnoreCase);
            AssertTexts(repository, x => x.Text == "alpha", "Alpha", "alpha", "ALPHA");
            AssertTexts(repository, x => x.Text == "CAFE", "Cafe");
            AssertTexts(repository, x => x.Text!.Contains("AF"), "Cafe", "Café", "café");
            AssertTexts(repository, x => x.Text!.StartsWith("zeb"), "Zebra");
            List<string> wanted = new List<string> { "ALPHA", "zebra" };
            AssertTexts(repository, x => wanted.Contains(x.Text!), "Alpha", "alpha", "ALPHA", "Zebra");
            AssertTexts(repository, x => x.Text != "ALPHA", "Cafe", "Café", "café", "Zebra", "50%_off", null);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes, Description = "Explicit OrdinalIgnoreCase / InvariantCultureIgnoreCase override the Database default")]
        public async Task ExplicitIgnoreCase()
        {
            IRepository<CfTextItem> repository = await SeedAsync(StringMatchMode.Database);
            AssertTexts(repository, x => x.Text!.Equals("ALPHA", StringComparison.OrdinalIgnoreCase), "Alpha", "alpha", "ALPHA");
            AssertTexts(repository, x => x.Text!.Contains("cafe", StringComparison.OrdinalIgnoreCase), "Cafe");
            AssertTexts(repository, x => x.Text!.StartsWith("CAF", StringComparison.InvariantCultureIgnoreCase), "Cafe", "Café", "café");
            AssertTexts(repository, x => string.Equals(x.Text, "zebra", StringComparison.OrdinalIgnoreCase), "Zebra");
        }

        [ConformanceTest(Requires = RepositoryCapabilities.StringMatchModes, Description = "The repository mode applies to query builders, counts and existence checks")]
        public async Task ModeAppliesEverywhere()
        {
            IRepository<CfTextItem> ordinal = await SeedAsync(StringMatchMode.Ordinal);
            Assert.Equal(1L, await ordinal.Query().Where(x => x.Text == "ALPHA").CountAsync(Token));
            Assert.Equal(1L, ordinal.Count(x => x.Text!.StartsWith("Z")));
            Assert.False(await ordinal.ExistsAsync(x => x.Text == "zebra", null, Token));
            IRepository<CfTextItem> ignoreCase = Repository<CfTextItem>(new RepositoryOptions { StringMatching = StringMatchMode.IgnoreCase });
            Assert.Equal(3L, await ignoreCase.Query().Where(x => x.Text == "ALPHA").CountAsync(Token));
            Assert.True(await ignoreCase.ExistsAsync(x => x.Text == "zebra", null, Token));
            Assert.Equal(2, ignoreCase.DeleteMany(x => x.Text == "CAFE" || x.Text == "zebra"));
        }

        private async Task<IRepository<CfTextItem>> SeedAsync(StringMatchMode mode)
        {
            await ResetAsync(typeof(CfTextItem));
            IRepository<CfTextItem> repository = Repository<CfTextItem>(new RepositoryOptions { StringMatching = mode });
            await repository.CreateManyAsync(_Values.Select(v => new CfTextItem { Text = v }).ToList(), null, Token);
            return repository;
        }

        private static void AssertTexts(IRepository<CfTextItem> repository, Expression<Func<CfTextItem, bool>> predicate, params string?[] expected)
        {
            List<string?> actual = repository.ReadMany(predicate).Select(x => x.Text).ToList();
            ConformanceAssert.NameSet(actual, expected, "ReadMany(" + predicate + ")");
        }
    }
}
