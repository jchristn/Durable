namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Predicate translation correctness with real data round-trips on every provider: enums (string and integer storage),
    /// culture invariance, special characters, LIKE wildcard escaping, case-insensitive comparisons, C# null semantics,
    /// NOT precedence, boolean members, collection membership, string/date/math functions, arithmetic and conditionals.
    /// Every expectation is cross-checked against the same predicate evaluated client-side (the C# oracle) where the
    /// predicate can be evaluated in memory.
    /// </summary>
    public class QueryTranslationTestSuite : IDisposable
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private const string Alpha = QueryTranslationFixture.Alpha;
        private const string Beta = QueryTranslationFixture.Beta;
        private const string OBrien = QueryTranslationFixture.OBrien;
        private const string Zoe = QueryTranslationFixture.Zoe;
        private const string Nihon = QueryTranslationFixture.Nihon;
        private const string Padded = QueryTranslationFixture.Padded;

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryTranslationTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the specific database. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public QueryTranslationTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Equality and inequality on an enum stored as a string.
        /// </summary>
        [Fact]
        public async Task EnumStoredAsStringEqualityAndInequality()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Status == QtStatus.Active, Alpha, Nihon, Padded);
            await CheckAsync(f, x => x.Status != QtStatus.Active, Beta, OBrien, Zoe);
            QtStatus wanted = QtStatus.Closed;
            await CheckAsync(f, x => x.Status == wanted, Zoe);
        }

        /// <summary>
        /// Enum stored as a string is persisted by name, so a raw comparison against the name matches.
        /// </summary>
        [Fact]
        public async Task EnumStoredAsStringIsPersistedByName()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().WhereRaw("status = {0}", "Suspended"), OBrien);
        }

        /// <summary>
        /// Equality and ordering comparisons on an enum stored as an integer.
        /// </summary>
        [Fact]
        public async Task EnumStoredAsIntegerEqualityAndComparison()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Priority == QtPriority.High, Alpha);
            await CheckAsync(f, x => x.Priority >= QtPriority.Medium, Alpha, OBrien, Zoe, Nihon);
            await CheckAsync(f, x => x.Priority < QtPriority.Medium, Beta, Padded);
        }

        /// <summary>
        /// Enum stored as an integer is persisted as its numeric value.
        /// </summary>
        [Fact]
        public async Task EnumStoredAsIntegerIsPersistedAsNumber()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().WhereRaw("priority = {0}", 4), OBrien);
        }

        /// <summary>
        /// Contains over a list/array of enums for both storage modes, plus the NotIn extension.
        /// </summary>
        [Fact]
        public async Task EnumCollectionContains()
        {
            using QueryTranslationFixture f = await SeedAsync();
            List<QtStatus> statuses = new List<QtStatus> { QtStatus.Draft, QtStatus.Closed };
            QtPriority[] priorities = new[] { QtPriority.Low, QtPriority.Critical };
            await CheckAsync(f, x => statuses.Contains(x.Status), Beta, Zoe);
            await CheckAsync(f, x => priorities.Contains(x.Priority), Beta, OBrien, Padded);
            await CheckAsync(f, x => !statuses.Contains(x.Status), Alpha, OBrien, Nihon, Padded);
            await CheckAsync(f, x => x.Status.NotIn(QtStatus.Active, QtStatus.Draft), OBrien, Zoe);
        }

        /// <summary>
        /// Decimal and double filters (and aggregates) produce correct rows while the current culture uses a comma decimal separator.
        /// </summary>
        [Fact]
        public async Task DecimalAndDoubleFiltersAreCultureInvariant()
        {
            using QueryTranslationFixture f = await SeedAsync();
            CultureInfo original = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");
                await CheckAsync(f, x => x.Price > 10.25m && x.Ratio < 1.0, Alpha, Zoe, Nihon);
                await CheckAsync(f, x => x.Price == 7.25m, OBrien);
                await CheckAsync(f, x => x.Ratio > 0.3 && x.Ratio < 0.6, Zoe);
                await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().WhereRaw("price > {0}", 50.5m), Zoe);
                decimal sum = await f.Items.Query().Where(x => x.Category == "Tools").SumAsync(x => x.Price);
                Assert.Equal(30.50m, sum);
            }
            finally
            {
                CultureInfo.CurrentCulture = original;
            }
        }

        /// <summary>
        /// Writing and reading decimals/doubles under a comma-decimal culture round-trips exactly.
        /// </summary>
        [Fact]
        public async Task DecimalAndDoubleRoundTripUnderCommaCulture()
        {
            using QueryTranslationFixture f = await SeedAsync();
            CultureInfo original = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = new CultureInfo("de-DE");
                QtItem created = await f.Items.CreateAsync(new QtItem
                {
                    Name = "Kultur", Price = 1234.56m, Ratio = 0.125, Quantity = 1, Category = "Culture",
                    CreatedUtc = new DateTime(2024, 5, 1), Status = QtStatus.Draft, Priority = QtPriority.Low
                });
                QtItem? read = await f.Items.ReadByIdAsync(created.Id);
                Assert.NotNull(read);
                Assert.Equal(1234.56m, read!.Price);
                Assert.Equal(0.125, read.Ratio);
                await QueryTranslationFixture.AssertNamesAsync(f.Items.Query().Where(x => x.Price == 1234.56m), "Kultur");
            }
            finally
            {
                CultureInfo.CurrentCulture = original;
            }
        }

        /// <summary>
        /// String equality with apostrophes, backslashes, newlines and non-ASCII text.
        /// </summary>
        [Fact]
        public async Task StringEqualityWithSpecialCharacters()
        {
            using QueryTranslationFixture f = await SeedAsync();
            string apostrophe = "O'Brien";
            await CheckAsync(f, x => x.Name == "O'Brien", OBrien);
            await CheckAsync(f, x => x.Name == apostrophe, OBrien);
            await CheckAsync(f, x => x.Code == "path\\to\\file", OBrien);
            await CheckAsync(f, x => x.Code == "line1\nline2", Nihon);
            await CheckAsync(f, x => x.Name == "Zoë", Zoe);
            await CheckAsync(f, x => x.Name == "日本", Nihon);
        }

        /// <summary>
        /// Contains/StartsWith/EndsWith with apostrophes, backslashes, newlines and non-ASCII text.
        /// </summary>
        [Fact]
        public async Task StringPatternMethodsWithSpecialCharacters()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Contains("'Bri"), OBrien);
            await CheckAsync(f, x => x.Name.StartsWith("O'"), OBrien);
            await CheckAsync(f, x => x.Name.EndsWith("'Brien"), OBrien);
            await CheckAsync(f, x => x.Code!.Contains("\\to\\"), OBrien);
            await CheckAsync(f, x => x.Code!.EndsWith("\\file"), OBrien);
            await CheckAsync(f, x => x.Code!.Contains("1\nl"), Nihon);
            await CheckAsync(f, x => x.Code!.StartsWith("line1\n"), Nihon);
            await CheckAsync(f, x => x.Name.StartsWith("日"), Nihon);
            await CheckAsync(f, x => x.Name.EndsWith("本"), Nihon);
            await CheckAsync(f, x => x.Name.EndsWith("oë"), Zoe);
        }

        /// <summary>
        /// A pattern argument with an accented character matches ordinally (like string.Contains): "ë" does not match "e".
        /// </summary>
        [Fact]
        public async Task StringPatternWithAccentIsOrdinal()
        {
            // String matching follows the database collation. MySQL's default utf8mb4_0900_ai_ci collation is
            // accent-insensitive, so this ordinal expectation only holds on the other providers.
            if (_Provider.DatabaseType == TestDatabaseType.MySql) return;
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Contains("ë"), Zoe);
        }

        /// <summary>
        /// A percent sign in a pattern argument matches literally.
        /// </summary>
        [Fact]
        public async Task LikePercentIsEscaped()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Code!.Contains("%"), Beta, Padded);
            await CheckAsync(f, x => x.Code!.Contains("0%"), Beta, Padded);
            await CheckAsync(f, x => x.Code!.EndsWith("0%"), Padded);
            await CheckAsync(f, x => x.Code!.StartsWith("50%"), Beta);
        }

        /// <summary>
        /// An underscore in a pattern argument matches literally (not as a single-character wildcard).
        /// </summary>
        [Fact]
        public async Task LikeUnderscoreIsEscaped()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Code!.Contains("_"), Beta);
            await CheckAsync(f, x => x.Code!.Contains("%_"), Beta);
            await CheckAsync(f, x => x.Code!.StartsWith("A_"));
        }

        /// <summary>
        /// Square brackets in a pattern argument match literally (they are a character class on SQL Server).
        /// </summary>
        [Fact]
        public async Task LikeBracketIsEscaped()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Code!.StartsWith("["), Zoe);
            await CheckAsync(f, x => x.Code!.Contains("[draft]"), Zoe);
            await CheckAsync(f, x => x.Code!.Contains("[a-z]"));
        }

        /// <summary>
        /// The LIKE escape character itself in a pattern argument matches literally.
        /// </summary>
        [Fact]
        public async Task LikeEscapeCharacterIsEscaped()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Code!.EndsWith("!"), Zoe);
            await CheckAsync(f, x => x.Code!.Contains("]!"), Zoe);
            await CheckAsync(f, x => x.Code!.Contains("!"), Zoe);
        }

        /// <summary>
        /// Case-insensitive Equals with StringComparison.OrdinalIgnoreCase (instance and static forms).
        /// </summary>
        [Fact]
        public async Task CaseInsensitiveEquals()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Equals("alpha", StringComparison.OrdinalIgnoreCase), Alpha);
            await CheckAsync(f, x => string.Equals(x.Email, "obrien@x.com", StringComparison.OrdinalIgnoreCase), OBrien);
            await CheckAsync(f, x => x.Name.Equals("o'BRIEN", StringComparison.OrdinalIgnoreCase), OBrien);
        }

        /// <summary>
        /// Case-insensitive Contains/StartsWith/EndsWith with StringComparison.OrdinalIgnoreCase.
        /// </summary>
        [Fact]
        public async Task CaseInsensitivePatternMethods()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Email != null && x.Email.Contains("obrien@", StringComparison.OrdinalIgnoreCase), OBrien);
            await CheckAsync(f, x => x.Name.StartsWith("ALP", StringComparison.OrdinalIgnoreCase), Alpha);
            await CheckAsync(f, x => x.Name.EndsWith("PHA", StringComparison.OrdinalIgnoreCase), Alpha);
            await CheckAsync(f, x => x.Name.Contains("'b", StringComparison.OrdinalIgnoreCase), OBrien);
        }

        /// <summary>
        /// Nullable HasValue and its negation.
        /// </summary>
        [Fact]
        public async Task NullableHasValue()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Discount.HasValue, Alpha, OBrien, Zoe, Padded);
            await CheckAsync(f, x => !x.Discount.HasValue, Beta, Nihon);
            await CheckAsync(f, x => x.DueDate.HasValue, Alpha, OBrien, Zoe, Padded);
        }

        /// <summary>
        /// Nullable .Value access in comparisons.
        /// </summary>
        [Fact]
        public async Task NullableValueAccess()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Discount.HasValue && x.Discount.Value > 1, Alpha, Zoe);
            await CheckAsync(f, x => x.Discount!.Value > 1, Alpha, Zoe);
        }

        /// <summary>
        /// == null with the null on either side, for value and reference types, including a captured null variable.
        /// </summary>
        [Fact]
        public async Task EqualsNullOnBothSides()
        {
            using QueryTranslationFixture f = await SeedAsync();
            string? noEmail = null;
            int? noDiscount = null;
            await CheckAsync(f, x => x.Discount == null, Beta, Nihon);
            await CheckAsync(f, x => null == x.Discount, Beta, Nihon);
            await CheckAsync(f, x => x.Email == null, Beta);
            await CheckAsync(f, x => null == x.Email, Beta);
            await CheckAsync(f, x => x.Email == noEmail, Beta);
            await CheckAsync(f, x => x.Discount == noDiscount, Beta, Nihon);
            await CheckAsync(f, x => x.DueDate != null, Alpha, OBrien, Zoe, Padded);
        }

        /// <summary>
        /// != against a value on a nullable column follows C# semantics: NULL rows are included.
        /// </summary>
        [Fact]
        public async Task NotEqualOnNullableColumnIncludesNulls()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Email != "alpha@x.com", Beta, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(f, x => x.Discount != 2, Beta, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(f, x => 0 != x.Discount, Alpha, Beta, Zoe, Nihon, Padded);
            await CheckAsync(f, x => x.IsFeatured != true, Beta, OBrien, Nihon, Padded);
        }

        /// <summary>
        /// != between two columns where one side is nullable follows C# semantics: NULL vs non-NULL is "not equal".
        /// </summary>
        [Fact]
        public async Task NotEqualBetweenColumnsIncludesNulls()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Discount != x.Quantity, Alpha, Beta, OBrien, Zoe, Nihon);
        }

        /// <summary>
        /// NOT over a comparison on a nullable column follows C# semantics: !(null &gt; 1) is true.
        /// </summary>
        [Fact]
        public async Task NotOverNullableComparisonIncludesNulls()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => !(x.Discount > 1), Beta, OBrien, Nihon, Padded);
        }

        /// <summary>
        /// The ?? operator translates to COALESCE for value and reference types.
        /// </summary>
        [Fact]
        public async Task CoalesceOperator()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => (x.Discount ?? 0) == 0, Beta, OBrien, Nihon);
            await CheckAsync(f, x => (x.Email ?? "none") == "none", Beta);
            await CheckAsync(f, x => (x.Discount ?? 100) > 4, Beta, Zoe, Nihon);
        }

        /// <summary>
        /// NOT applied to AND and OR groups keeps the grouping.
        /// </summary>
        [Fact]
        public async Task NotPrecedenceOverAndOr()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => !(x.IsActive && x.Price > 10), Beta, OBrien, Nihon, Padded);
            await CheckAsync(f, x => !(x.Category == "Tools" || x.Category == "Garden"), Nihon, Padded);
            await CheckAsync(f, x => !x.Name.StartsWith("A") && !x.IsActive, Beta, Nihon);
            await CheckAsync(f, x => !(x.IsActive || x.Price > 18) || x.Quantity == 3, Zoe, Nihon);
        }

        /// <summary>
        /// Boolean members as predicates (bare, negated, compared, combined).
        /// </summary>
        [Fact]
        public async Task BooleanMemberPredicates()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.IsActive, Alpha, OBrien, Zoe, Padded);
            await CheckAsync(f, x => !x.IsActive, Beta, Nihon);
            await CheckAsync(f, x => x.IsActive == false, Beta, Nihon);
            await CheckAsync(f, x => x.IsActive && x.IsFeatured == true, Alpha, Zoe);
            await CheckAsync(f, x => x.IsActive || x.Quantity == 0, Alpha, Beta, OBrien, Zoe, Padded);
        }

        /// <summary>
        /// Nullable boolean predicates.
        /// </summary>
        [Fact]
        public async Task NullableBooleanPredicates()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.IsFeatured == true, Alpha, Zoe);
            await CheckAsync(f, x => x.IsFeatured == false, OBrien, Padded);
            await CheckAsync(f, x => x.IsFeatured.HasValue, Alpha, OBrien, Zoe, Padded);
            await CheckAsync(f, x => x.IsFeatured!.Value, Alpha, Zoe);
        }

        /// <summary>
        /// Contains over List&lt;int&gt;, int[] and HashSet&lt;string&gt;.
        /// </summary>
        [Fact]
        public async Task CollectionContainsVariants()
        {
            using QueryTranslationFixture f = await SeedAsync();
            List<int> quantities = new List<int> { 5, 12, 99 };
            int[] quantityArray = new[] { 0, 3 };
            HashSet<string> categories = new HashSet<string> { "Tools", "Kitchen" };
            await CheckAsync(f, x => quantities.Contains(x.Quantity), Alpha, OBrien);
            await CheckAsync(f, x => quantityArray.Contains(x.Quantity), Beta, Zoe);
            await CheckAsync(f, x => categories.Contains(x.Category), Alpha, Beta, Nihon, Padded);
            await CheckAsync(f, x => !quantities.Contains(x.Quantity), Beta, Zoe, Nihon, Padded);
        }

        /// <summary>
        /// Contains over an empty collection returns no rows (and its negation returns all rows) without a SQL error.
        /// </summary>
        [Fact]
        public async Task EmptyCollectionContains()
        {
            using QueryTranslationFixture f = await SeedAsync();
            List<int> emptyList = new List<int>();
            int[] emptyArray = Array.Empty<int>();
            HashSet<string> emptySet = new HashSet<string>();
            await CheckAsync(f, x => emptyList.Contains(x.Quantity));
            await CheckAsync(f, x => emptyArray.Contains(x.Quantity));
            await CheckAsync(f, x => emptySet.Contains(x.Category));
            await CheckAsync(f, x => !emptyList.Contains(x.Quantity), Alpha, Beta, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(f, x => x.Quantity.In(emptyList));
            await CheckAsync(f, x => x.Quantity.NotIn(emptyList), Alpha, Beta, OBrien, Zoe, Nihon, Padded);
        }

        /// <summary>
        /// Contains over a list holding null matches NULL rows.
        /// </summary>
        [Fact]
        public async Task CollectionContainsWithNullElement()
        {
            using QueryTranslationFixture f = await SeedAsync();
            List<string?> emails = new List<string?> { null, "alpha@x.com" };
            await CheckAsync(f, x => emails.Contains(x.Email), Alpha, Beta);
        }

        /// <summary>
        /// Contains over a large integer list (above the inlining threshold) still matches.
        /// </summary>
        [Fact]
        public async Task LargeIntegerListContains()
        {
            using QueryTranslationFixture f = await SeedAsync();
            List<int> values = Enumerable.Range(1000, 600).ToList();
            values.Add(5);
            await CheckAsync(f, x => values.Contains(x.Quantity), Alpha);
        }

        /// <summary>
        /// ExpressionExtensions In / NotIn / Between, including NotIn on a nullable column (NULL rows are "not in").
        /// </summary>
        [Fact]
        public async Task InNotInBetweenExtensions()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Quantity.In(1, 3), Zoe, Padded);
            await CheckAsync(f, x => x.Quantity.NotIn(new List<int> { 1, 3 }), Alpha, Beta, OBrien, Nihon);
            await CheckAsync(f, x => x.Price.Between(10m, 20m), Alpha, Beta, Nihon);
            await CheckAsync(f, x => x.CreatedUtc.Between(new DateTime(2024, 1, 1), new DateTime(2024, 3, 1)), OBrien, Zoe);
            await CheckAsync(f, x => x.Discount.NotIn(new int?[] { 2, 5 }), Beta, OBrien, Nihon, Padded);
        }

        /// <summary>
        /// ToUpper/ToLower in predicates.
        /// </summary>
        [Fact]
        public async Task StringCaseFunctions()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.ToUpper() == "ALPHA", Alpha);
            await CheckAsync(f, x => x.Name.ToLower() == "o'brien", OBrien);
            await CheckAsync(f, x => x.Email!.ToLower() == "obrien@x.com", OBrien);
        }

        /// <summary>
        /// Trim in predicates.
        /// </summary>
        [Fact]
        public async Task StringTrim()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Trim() == "padded", Padded);
        }

        /// <summary>
        /// Length in predicates (character count, including non-ASCII).
        /// </summary>
        [Fact]
        public async Task StringLength()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Length == 2, Nihon);
            await CheckAsync(f, x => x.Name.Length == 3, Zoe);
            await CheckAsync(f, x => x.Name.Length == 4, Beta);
        }

        /// <summary>
        /// Length counts trailing whitespace like string.Length.
        /// </summary>
        [Fact]
        public async Task StringLengthCountsTrailingSpaces()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Length == 10, Padded);
            await CheckAsync(f, x => x.Name.Length > 6, OBrien, Padded);
        }

        /// <summary>
        /// Substring with and without a length argument.
        /// </summary>
        [Fact]
        public async Task StringSubstring()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Substring(1, 3) == "lph", Alpha);
            await CheckAsync(f, x => x.Name.Substring(2) == "ta", Beta);
        }

        /// <summary>
        /// Replace (ordinal, case-sensitive like string.Replace).
        /// </summary>
        [Fact]
        public async Task StringReplace()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.Replace("'", "") == "OBrien", OBrien);

            // REPLACE follows the database collation; SQL Server's default collation is case-insensitive, so the
            // case-sensitive expectation only holds on the other providers.
            if (_Provider.DatabaseType == TestDatabaseType.SqlServer) return;
            await CheckAsync(f, x => x.Name.Replace("a", "4") == "Alph4", Alpha);
        }

        /// <summary>
        /// IndexOf returns zero-based positions and -1 when absent.
        /// </summary>
        [Fact]
        public async Task StringIndexOf()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name.IndexOf("ph") == 2, Alpha);
            await CheckAsync(f, x => x.Name.IndexOf("'") == 1, OBrien);
            await CheckAsync(f, x => x.Name.IndexOf("zz") == -1, Alpha, Beta, OBrien, Zoe, Nihon, Padded);
        }

        /// <summary>
        /// string.IsNullOrEmpty distinguishes NULL/empty from whitespace-only.
        /// </summary>
        [Fact]
        public async Task StringIsNullOrEmpty()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => string.IsNullOrEmpty(x.Email), Beta, Nihon);
            await CheckAsync(f, x => !string.IsNullOrEmpty(x.Email), Alpha, OBrien, Zoe, Padded);
        }

        /// <summary>
        /// string.IsNullOrWhiteSpace matches NULL, empty and whitespace-only.
        /// </summary>
        [Fact]
        public async Task StringIsNullOrWhiteSpace()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => string.IsNullOrWhiteSpace(x.Email), Beta, Nihon, Padded);
        }

        /// <summary>
        /// String concatenation in predicates, including non-string operands and NULL (treated as empty like C#).
        /// </summary>
        [Fact]
        public async Task StringConcatenationInPredicate()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Name + ":" + x.Category == "Alpha:Tools", Alpha);
            await CheckAsync(f, x => x.Name + x.Quantity == "Alpha5", Alpha);
            await CheckAsync(f, x => x.Category + "/" + x.Email == "Tools/", Beta);
            await CheckAsync(f, x => string.Concat(x.Name, "-", x.Category) == "Zoë-Garden", Zoe);
        }

        /// <summary>
        /// DateTime Year/Month/Day/Hour/Minute parts.
        /// </summary>
        [Fact]
        public async Task DateTimeParts()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.CreatedUtc.Year == 2024, Alpha, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(f, x => x.CreatedUtc.Month == 3, Alpha, Padded);
            await CheckAsync(f, x => x.CreatedUtc.Day == 29, Zoe);
            await CheckAsync(f, x => x.CreatedUtc.Hour == 10, Alpha);
            await CheckAsync(f, x => x.CreatedUtc.Minute == 15, Zoe);
            await CheckAsync(f, x => x.DueDate.HasValue && x.DueDate.Value.Month == 3, Zoe, Padded);
        }

        /// <summary>
        /// DateTime DayOfWeek uses .NET numbering (Sunday = 0).
        /// </summary>
        [Fact]
        public async Task DateTimeDayOfWeek()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Friday, Alpha);
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Thursday, Zoe, Nihon);
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Sunday, Beta);
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Saturday, Padded);
        }

        /// <summary>
        /// DateTime.Date comparisons.
        /// </summary>
        [Fact]
        public async Task DateTimeDateComparison()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.CreatedUtc.Date == new DateTime(2024, 3, 15), Alpha);
            await CheckAsync(f, x => x.CreatedUtc.Date < new DateTime(2024, 1, 1), Beta);
            await CheckAsync(f, x => x.CreatedUtc.Date == new DateTime(2024, 1, 1), OBrien);
        }

        /// <summary>
        /// AddDays/AddHours/AddMonths/AddYears on a column inside predicates.
        /// </summary>
        [Fact]
        public async Task DateTimeAddMethodsInPredicate()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.CreatedUtc.AddDays(-1) < new DateTime(2024, 1, 1), Beta, OBrien);
            await CheckAsync(f, x => x.CreatedUtc.AddDays(1) > new DateTime(2024, 7, 5), Nihon);
            await CheckAsync(f, x => x.CreatedUtc.AddHours(14).Day == 16, Alpha, Padded);
            await CheckAsync(f, x => x.CreatedUtc.AddMonths(1).Month == 4, Alpha, Padded);
            await CheckAsync(f, x => x.CreatedUtc.AddYears(1).Year == 2025, Alpha, OBrien, Zoe, Nihon, Padded);
        }

        /// <summary>
        /// DateTime.UtcNow and captured DateTime variables are evaluated client-side and bound as parameters.
        /// </summary>
        [Fact]
        public async Task DateTimeUtcNowAndCapturedValues()
        {
            using QueryTranslationFixture f = await SeedAsync();
            DateTime cutoff = new DateTime(2024, 3, 1);
            await CheckAsync(f, x => x.CreatedUtc < DateTime.UtcNow, Alpha, Beta, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(f, x => x.DueDate > DateTime.UtcNow);
            await CheckAsync(f, x => x.CreatedUtc > DateTime.UtcNow.AddYears(-100), Alpha, Beta, OBrien, Zoe, Nihon, Padded);
            await CheckAsync(f, x => x.CreatedUtc >= cutoff, Alpha, Nihon, Padded);
        }

        /// <summary>
        /// Arithmetic operators in predicates.
        /// </summary>
        [Fact]
        public async Task ArithmeticOperators()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Price * x.Quantity > 50, Alpha, OBrien, Zoe, Nihon);
            await CheckAsync(f, x => x.Quantity % 2 == 0, Beta, OBrien, Nihon);
            await CheckAsync(f, x => (x.Quantity + 1) * 2 == 12, Alpha);
            await CheckAsync(f, x => x.Quantity - x.Discount == 3, Alpha);
            await CheckAsync(f, x => -x.Ratio > 1, Nihon);
            await CheckAsync(f, x => x.Price - 0.5m == 10m, Alpha);
        }

        /// <summary>
        /// Integer division truncates like C#.
        /// </summary>
        [Fact]
        public async Task IntegerDivisionTruncates()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => x.Quantity / 2 == 2, Alpha);
            await CheckAsync(f, x => x.Quantity / 5 == 1, Alpha, Nihon);
        }

        /// <summary>
        /// Math.Abs/Round/Floor/Ceiling on decimal and double columns (avoiding midpoint values).
        /// </summary>
        [Fact]
        public async Task MathFunctions()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => Math.Abs(x.Ratio) > 2, OBrien);
            await CheckAsync(f, x => Math.Abs(x.Quantity - 10) <= 2, OBrien, Nihon);
            await CheckAsync(f, x => Math.Round(x.Price) == 100, Zoe);
            await CheckAsync(f, x => Math.Round(x.Price, 1) == 100.0m, Zoe);
            await CheckAsync(f, x => Math.Floor(x.Price) == 7, OBrien);
            await CheckAsync(f, x => Math.Ceiling(x.Price) == 11, Alpha);
            await CheckAsync(f, x => Math.Floor(x.Ratio) == -2, Nihon);
            await CheckAsync(f, x => Math.Ceiling(x.Ratio) == -1, Nihon);
        }

        /// <summary>
        /// Conditional (ternary) expressions inside predicates.
        /// </summary>
        [Fact]
        public async Task TernaryInPredicate()
        {
            using QueryTranslationFixture f = await SeedAsync();
            await CheckAsync(f, x => (x.IsActive ? x.Price : 0m) > 50, Zoe);
            await CheckAsync(f, x => (x.Discount.HasValue ? x.Discount.Value : -1) == -1, Beta, Nihon);
            await CheckAsync(f, x => (x.Category == "Tools" ? "T" : "X") == "T", Alpha, Beta);
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private Task<QueryTranslationFixture> SeedAsync()
        {
            return QueryTranslationFixture.CreateAsync(_Provider);
        }

        private static async Task CheckAsync(QueryTranslationFixture fixture, Expression<Func<QtItem, bool>> predicate, params string[] expected)
        {
            VerifyOracle(fixture, predicate, expected);
            await QueryTranslationFixture.AssertNamesAsync(fixture.Items.Query().Where(predicate), expected);
        }

        private static void VerifyOracle(QueryTranslationFixture fixture, Expression<Func<QtItem, bool>> predicate, string[] expected)
        {
            Func<QtItem, bool> compiled = predicate.Compile();
            string[] clientNames;
            try
            {
                clientNames = fixture.NamesWhere(compiled);
            }
            catch (NullReferenceException)
            {
                return;
            }
            catch (InvalidOperationException)
            {
                return;
            }
            catch (ArgumentException)
            {
                return;
            }

            QueryTranslationFixture.AssertNameSet(clientNames, expected, "client-side oracle disagrees with test expectation for " + predicate);
        }

        #endregion
    }
}
