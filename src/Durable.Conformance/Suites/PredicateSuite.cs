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
    /// Predicate translation with C# semantics, needing no optional capability: comparisons on every scalar type, boolean
    /// logic and precedence, C# null semantics (including negation and column-to-column comparisons), enums in both storage
    /// modes, collection membership (empty lists, nulls, large lists, In/NotIn/Between), string equality and
    /// Contains/StartsWith/EndsWith with special characters, IsNullOrEmpty/IsNullOrWhiteSpace, concatenation, arithmetic,
    /// coalesce and conditionals. Every expectation is cross-checked against LINQ-to-objects.
    /// </summary>
    internal sealed class PredicateSuite : KitSuite
    {
        private const string A = ItemFixture.Alpha;
        private const string B = ItemFixture.Beta;
        private const string O = ItemFixture.OBrien;
        private const string D = ItemFixture.Delta;
        private const string E = ItemFixture.Echo;
        private const string F = ItemFixture.Foxtrot;

        public PredicateSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "==, !=, <, <=, >, >= on int, constants on either side, captured variables")]
        public async Task IntegerComparisons()
        {
            ItemFixture f = await SeedItemsAsync();
            int minimum = 8;
            await CheckAsync(f, x => x.Quantity == 5, A);
            await CheckAsync(f, x => x.Quantity != 5, B, O, D, E, F);
            await CheckAsync(f, x => x.Quantity > 5, O, E);
            await CheckAsync(f, x => x.Quantity >= 5, A, O, E);
            await CheckAsync(f, x => x.Quantity < 3, B, F);
            await CheckAsync(f, x => x.Quantity <= 3, B, D, F);
            await CheckAsync(f, x => 3 < x.Quantity, A, O, E);
            await CheckAsync(f, x => x.Quantity >= minimum, O, E);
        }

        [ConformanceTest(Description = "Comparisons on decimal and double columns")]
        public async Task DecimalAndDoubleComparisons()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Price > 10.25m, A, B, D, E);
            await CheckAsync(f, x => x.Price == 7.25m, O);
            await CheckAsync(f, x => x.Price <= 0.5m, F);
            await CheckAsync(f, x => x.Ratio < 0, E);
            await CheckAsync(f, x => x.Ratio > 0.3 && x.Ratio < 0.6, D);
            await CheckAsync(f, x => x.Ratio >= 1.5, B, O);
        }

        [ConformanceTest(Description = "Comparisons on 64-bit values beyond the int range")]
        public async Task LongComparisons()
        {
            ItemFixture f = await SeedItemsAsync();
            long threshold = 4000000000L;
            await CheckAsync(f, x => x.BigNumber > threshold, A, E);
            await CheckAsync(f, x => x.BigNumber < 0, B);
            await CheckAsync(f, x => x.BigNumber == 42L, D);
            await CheckAsync(f, x => x.BigNumber == 9000000000L, E);
        }

        [ConformanceTest(Description = "Boolean members bare, negated, compared and combined")]
        public async Task BooleanMembers()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.IsActive, A, O, D, F);
            await CheckAsync(f, x => !x.IsActive, B, E);
            await CheckAsync(f, x => x.IsActive == false, B, E);
            await CheckAsync(f, x => x.IsActive != true, B, E);
            await CheckAsync(f, x => x.IsActive && x.Quantity > 4, A, O);
            await CheckAsync(f, x => x.IsActive || x.Quantity == 0, A, B, O, D, F);
        }

        [ConformanceTest(Description = "Nullable booleans follow C# semantics (null != true)")]
        public async Task NullableBooleans()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.IsFeatured == true, A, D);
            await CheckAsync(f, x => x.IsFeatured == false, O, F);
            await CheckAsync(f, x => x.IsFeatured != true, B, O, E, F);
            await CheckAsync(f, x => x.IsFeatured.HasValue, A, O, D, F);
            await CheckAsync(f, x => x.IsFeatured == null, B, E);
            await CheckAsync(f, x => x.IsFeatured!.Value, A, D);
        }

        [ConformanceTest(Description = "DateTime comparisons with literals, captured values and nullable columns")]
        public async Task DateTimeComparisons()
        {
            ItemFixture f = await SeedItemsAsync();
            DateTime exact = new DateTime(2024, 2, 29, 8, 15, 45);
            await CheckAsync(f, x => x.CreatedUtc >= new DateTime(2024, 3, 1), A, E, F);
            await CheckAsync(f, x => x.CreatedUtc < new DateTime(2024, 1, 1), B);
            await CheckAsync(f, x => x.CreatedUtc == exact, D);
            await CheckAsync(f, x => x.DueDate > new DateTime(2024, 3, 10), A, F);
            await CheckAsync(f, x => x.DueDate == null, B, E);
            await CheckAsync(f, x => x.CreatedUtc < DateTime.UtcNow, A, B, O, D, E, F);
        }

        [ConformanceTest(Description = "Guid equality and inequality")]
        public async Task GuidComparisons()
        {
            ItemFixture f = await SeedItemsAsync();
            Guid token = new Guid("33333333-3333-3333-3333-333333333333");
            await CheckAsync(f, x => x.Token == token, O);
            await CheckAsync(f, x => x.Token != token, A, B, D, E, F);
            await CheckAsync(f, x => x.Token == Guid.Empty);
        }

        [ConformanceTest(Description = "Enum stored by name: equality, inequality, captured value")]
        public async Task EnumStoredAsString()
        {
            ItemFixture f = await SeedItemsAsync();
            CfStatus wanted = CfStatus.Closed;
            await CheckAsync(f, x => x.Status == CfStatus.Active, A, E, F);
            await CheckAsync(f, x => x.Status != CfStatus.Active, B, O, D);
            await CheckAsync(f, x => x.Status == wanted, D);
        }

        [ConformanceTest(Description = "Enum stored as an integer: equality and ordering comparisons")]
        public async Task EnumStoredAsInteger()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Priority == CfPriority.High, A);
            await CheckAsync(f, x => x.Priority >= CfPriority.Medium, A, O, D, E);
            await CheckAsync(f, x => x.Priority < CfPriority.Medium, B, F);
            await CheckAsync(f, x => x.Priority != CfPriority.Low, A, O, D, E);
        }

        [ConformanceTest(Description = "Contains over enum lists/arrays (both storage modes) and In/NotIn")]
        public async Task EnumMembership()
        {
            ItemFixture f = await SeedItemsAsync();
            List<CfStatus> statuses = new List<CfStatus> { CfStatus.Draft, CfStatus.Closed };
            CfPriority[] priorities = new[] { CfPriority.Low, CfPriority.Critical };
            await CheckAsync(f, x => statuses.Contains(x.Status), B, D);
            await CheckAsync(f, x => priorities.Contains(x.Priority), B, O, F);
            await CheckAsync(f, x => !statuses.Contains(x.Status), A, O, E, F);
            await CheckAsync(f, x => x.Status.NotIn(CfStatus.Active, CfStatus.Draft), O, D);
            await CheckAsync(f, x => x.Priority.In(CfPriority.Medium), D, E);
        }

        [ConformanceTest(Description = "== null with the null on either side, captured nulls, != null")]
        public async Task EqualsNull()
        {
            ItemFixture f = await SeedItemsAsync();
            string? noEmail = null;
            int? noDiscount = null;
            await CheckAsync(f, x => x.Discount == null, B, E);
            await CheckAsync(f, x => null == x.Discount, B, E);
            await CheckAsync(f, x => x.Email == null, B);
            await CheckAsync(f, x => null == x.Email, B);
            await CheckAsync(f, x => x.Email == noEmail, B);
            await CheckAsync(f, x => x.Discount == noDiscount, B, E);
            await CheckAsync(f, x => x.DueDate != null, A, O, D, F);
            await CheckAsync(f, x => x.Email != null, A, O, D, E, F);
        }

        [ConformanceTest(Description = "!= against a value on a nullable column includes null rows (C# semantics)")]
        public async Task NotEqualIncludesNulls()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Email != "alpha@x.com", B, O, D, E, F);
            await CheckAsync(f, x => x.Discount != 2, B, O, D, E, F);
            await CheckAsync(f, x => 0 != x.Discount, A, B, D, E, F);
        }

        [ConformanceTest(Description = "!= between a nullable and a non-nullable column treats null as different")]
        public async Task NotEqualBetweenColumns()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Discount != x.Quantity, A, B, O, D, E);
            await CheckAsync(f, x => x.Discount == x.Quantity, F);
        }

        [ConformanceTest(Description = "NOT over a comparison on a nullable column includes nulls: !(null > 1) is true")]
        public async Task NegatedNullableComparison()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => !(x.Discount > 1), B, O, E, F);
            await CheckAsync(f, x => !(x.DueDate < new DateTime(2024, 3, 10)), A, B, E, F);
        }

        [ConformanceTest(Description = "Nullable HasValue / Value")]
        public async Task NullableHasValueAndValue()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Discount.HasValue, A, O, D, F);
            await CheckAsync(f, x => !x.Discount.HasValue, B, E);
            await CheckAsync(f, x => x.Discount.HasValue && x.Discount.Value > 1, A, D);
            await CheckAsync(f, x => x.Discount!.Value > 1, A, D);
        }

        [ConformanceTest(Description = "The ?? operator on value and reference types")]
        public async Task CoalesceOperator()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => (x.Discount ?? 0) == 0, B, O, E);
            await CheckAsync(f, x => (x.Email ?? "none") == "none", B);
            await CheckAsync(f, x => (x.Discount ?? 100) > 4, B, D, E);
        }

        [ConformanceTest(Description = "NOT keeps AND/OR grouping")]
        public async Task NotPrecedence()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => !(x.IsActive && x.Price > 10), B, O, E, F);
            await CheckAsync(f, x => !(x.Category == "Tools" || x.Category == "Garden"), E, F);
            await CheckAsync(f, x => !x.Name.StartsWith("A") && !x.IsActive, B, E);
            await CheckAsync(f, x => !(x.IsActive || x.Price > 18) || x.Quantity == 3, D, E);
        }

        [ConformanceTest(Description = "AND binds tighter than OR; parentheses change the result")]
        public async Task AndOrGrouping()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.IsActive && x.Category == "Kitchen" || x.Category == "Tools", A, B, F);
            await CheckAsync(f, x => x.IsActive && (x.Category == "Kitchen" || x.Category == "Tools"), A, F);
            await CheckAsync(f, x => (x.Quantity > 4 || x.Price > 50) && (x.Category == "Garden" || x.IsActive == false), O, D, E);
        }

        [ConformanceTest(Description = "Contains over List<int>, int[] and HashSet<string>, and its negation")]
        public async Task CollectionContains()
        {
            ItemFixture f = await SeedItemsAsync();
            List<int> quantities = new List<int> { 5, 12, 99 };
            int[] quantityArray = new[] { 0, 3 };
            HashSet<string> categories = new HashSet<string> { "Tools", "Kitchen" };
            await CheckAsync(f, x => quantities.Contains(x.Quantity), A, O);
            await CheckAsync(f, x => quantityArray.Contains(x.Quantity), B, D);
            await CheckAsync(f, x => categories.Contains(x.Category), A, B, E, F);
            await CheckAsync(f, x => !quantities.Contains(x.Quantity), B, D, E, F);
        }

        [ConformanceTest(Description = "Contains over an empty collection matches nothing; its negation matches everything")]
        public async Task EmptyCollectionContains()
        {
            ItemFixture f = await SeedItemsAsync();
            List<int> emptyList = new List<int>();
            int[] emptyArray = Array.Empty<int>();
            HashSet<string> emptySet = new HashSet<string>();
            await CheckAsync(f, x => emptyList.Contains(x.Quantity));
            await CheckAsync(f, x => emptyArray.Contains(x.Quantity));
            await CheckAsync(f, x => emptySet.Contains(x.Category));
            await CheckAsync(f, x => !emptyList.Contains(x.Quantity), A, B, O, D, E, F);
            await CheckAsync(f, x => x.Quantity.In(emptyList));
            await CheckAsync(f, x => x.Quantity.NotIn(emptyList), A, B, O, D, E, F);
        }

        [ConformanceTest(Description = "Contains over a list holding null matches null rows; the negation excludes them")]
        public async Task CollectionContainsNull()
        {
            ItemFixture f = await SeedItemsAsync();
            List<string?> emails = new List<string?> { null, "alpha@x.com" };
            List<int?> discounts = new List<int?> { null, 5 };
            await CheckAsync(f, x => emails.Contains(x.Email), A, B);
            await CheckAsync(f, x => !emails.Contains(x.Email), O, D, E, F);
            await CheckAsync(f, x => discounts.Contains(x.Discount), B, D, E);
        }

        [ConformanceTest(Description = "Contains over a large list (600 values)")]
        public async Task LargeCollectionContains()
        {
            ItemFixture f = await SeedItemsAsync();
            List<int> values = Enumerable.Range(1000, 600).ToList();
            values.Add(5);
            await CheckAsync(f, x => values.Contains(x.Quantity), A);
            await CheckAsync(f, x => !values.Contains(x.Quantity), B, O, D, E, F);
        }

        [ConformanceTest(Description = "ExpressionExtensions In / NotIn / Between (inclusive)")]
        public async Task InNotInBetween()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Quantity.In(1, 3), D, F);
            await CheckAsync(f, x => x.Quantity.NotIn(new List<int> { 1, 3 }), A, B, O, E);
            await CheckAsync(f, x => x.Price.Between(10m, 20m), A, B, E);
            await CheckAsync(f, x => x.CreatedUtc.Between(new DateTime(2024, 1, 1), new DateTime(2024, 3, 1)), O, D);
            await CheckAsync(f, x => x.Discount.NotIn(new int?[] { 2, 5 }), B, O, E, F);
            await CheckAsync(f, x => x.Category.In("Garden", "Kitchen"), O, D, E, F);
        }

        [ConformanceTest(Description = "String equality with apostrophes, backslashes, brackets and newlines")]
        public async Task StringEqualitySpecialCharacters()
        {
            ItemFixture f = await SeedItemsAsync();
            string apostrophe = "O'Brien";
            await CheckAsync(f, x => x.Name == "O'Brien", O);
            await CheckAsync(f, x => x.Name == apostrophe, O);
            await CheckAsync(f, x => x.Code == "path\\to\\file", O);
            await CheckAsync(f, x => x.Code == "line1\nline2", E);
            await CheckAsync(f, x => x.Code == "[draft]!", D);
            await CheckAsync(f, x => x.Name.Equals("Echo"), E);
            await CheckAsync(f, x => x.Name != "Echo", A, B, O, D, F);
        }

        [ConformanceTest(Description = "Contains / StartsWith / EndsWith, including special characters and captured patterns")]
        public async Task StringPatternMethods()
        {
            ItemFixture f = await SeedItemsAsync();
            string prefix = "Fox";
            await CheckAsync(f, x => x.Name.StartsWith("Al"), A);
            await CheckAsync(f, x => x.Name.EndsWith("ta"), B, D);
            await CheckAsync(f, x => x.Name.Contains("ph"), A);
            await CheckAsync(f, x => x.Name.StartsWith(prefix), F);
            await CheckAsync(f, x => x.Name.Contains("'Bri"), O);
            await CheckAsync(f, x => x.Code!.Contains("\\to\\"), O);
            await CheckAsync(f, x => x.Code!.EndsWith("\\file"), O);
            await CheckAsync(f, x => x.Code!.Contains("1\nl"), E);
            await CheckAsync(f, x => x.Code!.StartsWith("line1\n"), E);
            await CheckAsync(f, x => x.Email!.EndsWith("@x.com"), A, O, D);
            await CheckAsync(f, x => !x.Name.Contains("a"), O, E, F);
        }

        [ConformanceTest(Description = "LIKE-style wildcard characters in patterns match literally: % _ [ ] ! \\")]
        public async Task PatternWildcardsAreLiteral()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Code!.Contains("%"), B, F);
            await CheckAsync(f, x => x.Code!.Contains("0%"), B, F);
            await CheckAsync(f, x => x.Code!.EndsWith("%  "), F);
            await CheckAsync(f, x => x.Code!.StartsWith("50%"), B);
            await CheckAsync(f, x => x.Code!.Contains("_"), B);
            await CheckAsync(f, x => x.Code!.Contains("%_"), B);
            await CheckAsync(f, x => x.Code!.StartsWith("A_"));
            await CheckAsync(f, x => x.Code!.StartsWith("["), D);
            await CheckAsync(f, x => x.Code!.Contains("[draft]"), D);
            await CheckAsync(f, x => x.Code!.Contains("[a-z]"));
            await CheckAsync(f, x => x.Code!.EndsWith("!"), D);
            await CheckAsync(f, x => x.Code!.Contains("]!"), D);
            await CheckAsync(f, x => x.Code!.Contains("\\"), O);
        }

        [ConformanceTest(Description = "string.IsNullOrEmpty / IsNullOrWhiteSpace distinguish null, empty and whitespace")]
        public async Task NullOrEmptyTests()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => string.IsNullOrEmpty(x.Email), B, E);
            await CheckAsync(f, x => !string.IsNullOrEmpty(x.Email), A, O, D, F);
            await CheckAsync(f, x => string.IsNullOrWhiteSpace(x.Email), B, E, F);
            await CheckAsync(f, x => !string.IsNullOrWhiteSpace(x.Email), A, O, D);
        }

        [ConformanceTest(Description = "String concatenation in predicates; null concatenates as empty")]
        public async Task StringConcatenation()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Name + ":" + x.Category == "Alpha:Tools", A);
            await CheckAsync(f, x => x.Name + x.Quantity == "Alpha5", A);
            await CheckAsync(f, x => x.Category + "/" + x.Email == "Tools/", B);
            await CheckAsync(f, x => string.Concat(x.Name, "-", x.Category) == "Delta-Garden", D);
        }

        [ConformanceTest(Description = "Arithmetic operators: + - * / % and unary minus")]
        public async Task ArithmeticOperators()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Price * x.Quantity > 50, A, O, D, E);
            await CheckAsync(f, x => x.Quantity % 2 == 0, B, O, E);
            await CheckAsync(f, x => (x.Quantity + 1) * 2 == 12, A);
            await CheckAsync(f, x => x.Quantity - x.Discount == 3, A);
            await CheckAsync(f, x => -x.Ratio > 1, E);
            await CheckAsync(f, x => x.Price - 0.5m == 10m, A);
            await CheckAsync(f, x => x.Price / 2 == 5.25m, A);
        }

        [ConformanceTest(Description = "Integer division truncates like C#")]
        public async Task IntegerDivision()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Quantity / 2 == 2, A);
            await CheckAsync(f, x => x.Quantity / 5 == 1, A, E);
            await CheckAsync(f, x => x.Quantity / 4 == 0, B, D, F);
        }

        [ConformanceTest(Description = "Conditional (ternary) expressions")]
        public async Task TernaryExpressions()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => (x.IsActive ? x.Price : 0m) > 50, D);
            await CheckAsync(f, x => (x.Discount.HasValue ? x.Discount.Value : -1) == -1, B, E);
            await CheckAsync(f, x => (x.Category == "Tools" ? "T" : "X") == "T", A, B);
        }

        [ConformanceTest(Description = "Captured locals, array elements, members and method calls are evaluated client-side")]
        public async Task CapturedValuesAreEvaluatedClientSide()
        {
            ItemFixture f = await SeedItemsAsync();
            int[] ids = new[] { f.Item(A).Id, f.Item(D).Id };
            string[] names = new[] { E, F };
            await CheckAsync(f, x => x.Id == ids[1], D);
            await CheckAsync(f, x => x.Name == names[0], E);
            await CheckAsync(f, x => x.Id == f.Item(O).Id, O);
            await CheckAsync(f, x => x.Name == F.ToUpperInvariant().Substring(0, 1) + "oxtrot", F);
            await CheckAsync(f, x => x.CreatedUtc > DateTime.UtcNow.AddYears(-100), A, B, O, D, E, F);
            await CheckAsync(f, x => x.Price > decimal.Parse("50", System.Globalization.CultureInfo.InvariantCulture), D);
        }

        [ConformanceTest(Description = "Constant true/false predicates and constant sub-expressions")]
        public async Task ConstantPredicates()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => true, A, B, O, D, E, F);
            await CheckAsync(f, x => false);
            await CheckAsync(f, x => x.Quantity > 4 && true, A, O, E);
            await CheckAsync(f, x => 1 == 2 || x.Name == "Echo", E);
        }

        [ConformanceTest(Description = "ReadMany, Count, Exists, ReadFirst and Query().Where agree on the same predicate")]
        public async Task RepositoryMethodsShareSemantics()
        {
            ItemFixture f = await SeedItemsAsync();
            Expression<Func<CfItem, bool>> predicate = x => (x.Discount == null || x.Discount > 1) && x.Name != "Delta";
            string[] expected = new[] { A, B, E };
            f.VerifyOracle(predicate, expected);
            ConformanceAssert.NameSet(f.Items.ReadMany(predicate).Select(x => x.Name), expected, "ReadMany(" + predicate + ")");
            Assert.Equal(3L, f.Items.Count(predicate));
            Assert.Equal(3L, await f.Items.CountAsync(predicate, null, Token));
            Assert.True(f.Items.Exists(predicate));
            Assert.Contains(f.Items.ReadFirst(predicate)?.Name, expected);
            Assert.Equal(3L, f.Items.Query().Where(predicate).Count());
        }
    }
}
