namespace Durable.Conformance
{
    using System;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// String, date and math functions in predicates (<see cref="RepositoryCapabilities.Functions"/>), evaluated with .NET
    /// semantics (zero-based indexes, Length counting trailing spaces, Sunday = 0, truncating Floor/Ceiling). Midpoint
    /// rounding is avoided because SQL ROUND and Math.Round legitimately differ there.
    /// </summary>
    internal sealed class FunctionSuite : KitSuite
    {
        private const string A = ItemFixture.Alpha;
        private const string B = ItemFixture.Beta;
        private const string O = ItemFixture.OBrien;
        private const string D = ItemFixture.Delta;
        private const string E = ItemFixture.Echo;
        private const string F = ItemFixture.Foxtrot;

        public FunctionSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "ToUpper / ToLower")]
        public async Task CaseFunctions()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Name.ToUpper() == "ALPHA", A);
            await CheckAsync(f, x => x.Name.ToLower() == "o'brien", O);
            await CheckAsync(f, x => x.Email != null && x.Email.ToUpper() == "DELTA@X.COM", D);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "Trim / TrimStart / TrimEnd")]
        public async Task TrimFunctions()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Code!.Trim() == "100%", F);
            await CheckAsync(f, x => x.Code!.TrimStart() == "100%  ", F);
            await CheckAsync(f, x => x.Code!.TrimEnd() == "  100%", F);
            await CheckAsync(f, x => x.Email != null && x.Email.Trim().Length == 0, E, F);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "Length counts characters including leading and trailing spaces")]
        public async Task LengthFunction()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Name.Length == 4, B, E);
            await CheckAsync(f, x => x.Name.Length == 7, O, F);
            await CheckAsync(f, x => x.Code!.Length == 8, D, F);
            await CheckAsync(f, x => x.Email!.Length == 3, F);
            await CheckAsync(f, x => x.Name.Length > x.Quantity, B, D, F);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "Substring with and without a length")]
        public async Task SubstringFunction()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Name.Substring(1, 3) == "lph", A);
            await CheckAsync(f, x => x.Name.Substring(2) == "ta", B);
            await CheckAsync(f, x => x.Name.Substring(0, 1) == "D", D);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "Replace")]
        public async Task ReplaceFunction()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Name.Replace("'", "") == "OBrien", O);
            await CheckAsync(f, x => x.Code!.Replace("%", "pct") == "50pct_off", B);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "IndexOf is zero-based and -1 when absent")]
        public async Task IndexOfFunction()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.Name.IndexOf("ph") == 2, A);
            await CheckAsync(f, x => x.Name.IndexOf("'") == 1, O);
            await CheckAsync(f, x => x.Name.IndexOf("zz") == -1, A, B, O, D, E, F);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "Year / Month / Day / Hour / Minute / Second, including nullable dates")]
        public async Task DateParts()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.CreatedUtc.Year == 2024, A, O, D, E, F);
            await CheckAsync(f, x => x.CreatedUtc.Month == 3, A, F);
            await CheckAsync(f, x => x.CreatedUtc.Day == 29, D);
            await CheckAsync(f, x => x.CreatedUtc.Hour == 10, A);
            await CheckAsync(f, x => x.CreatedUtc.Minute == 15, D);
            await CheckAsync(f, x => x.CreatedUtc.Second == 45, D);
            await CheckAsync(f, x => x.DueDate.HasValue && x.DueDate.Value.Month == 3, D, F);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "DayOfWeek uses .NET numbering (Sunday = 0); DayOfYear")]
        public async Task DayOfWeekAndDayOfYear()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Friday, A);
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Thursday, D, E);
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Sunday, B);
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Saturday, F);
            await CheckAsync(f, x => x.CreatedUtc.DayOfWeek == DayOfWeek.Monday, O);
            await CheckAsync(f, x => x.CreatedUtc.DayOfYear == 1, O);
            await CheckAsync(f, x => x.CreatedUtc.DayOfYear == 60, D);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "DateTime.Date comparisons")]
        public async Task DatePart()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.CreatedUtc.Date == new DateTime(2024, 3, 15), A);
            await CheckAsync(f, x => x.CreatedUtc.Date < new DateTime(2024, 1, 1), B);
            await CheckAsync(f, x => x.CreatedUtc.Date == new DateTime(2024, 1, 1), O);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "AddYears / AddMonths / AddDays / AddHours / AddMinutes / AddSeconds on columns")]
        public async Task DateArithmetic()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => x.CreatedUtc.AddDays(-1) < new DateTime(2024, 1, 1), B, O);
            await CheckAsync(f, x => x.CreatedUtc.AddDays(1) > new DateTime(2024, 7, 5), E);
            await CheckAsync(f, x => x.CreatedUtc.AddHours(14).Day == 16, A, F);
            await CheckAsync(f, x => x.CreatedUtc.AddMonths(1).Month == 4, A, F);
            await CheckAsync(f, x => x.CreatedUtc.AddYears(1).Year == 2025, A, O, D, E, F);
            await CheckAsync(f, x => x.CreatedUtc.AddMinutes(30).Hour == 11, A);
            await CheckAsync(f, x => x.CreatedUtc.AddSeconds(15).Minute == 16, D);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Functions, Description = "Math.Abs / Round (non-midpoint) / Floor / Ceiling / Sqrt / Pow")]
        public async Task MathFunctions()
        {
            ItemFixture f = await SeedItemsAsync();
            await CheckAsync(f, x => Math.Abs(x.Ratio) > 2, O);
            await CheckAsync(f, x => Math.Abs(x.Quantity - 10) <= 2, O, E);
            await CheckAsync(f, x => Math.Round(x.Price / 3, 2) == 33.25m, D);
            await CheckAsync(f, x => Math.Round(x.Price / 3, 2) == 2.42m, O);
            await CheckAsync(f, x => Math.Floor(x.Price) == 7, O);
            await CheckAsync(f, x => Math.Ceiling(x.Price) == 11, A);
            await CheckAsync(f, x => Math.Floor(x.Ratio) == -2, E);
            await CheckAsync(f, x => Math.Ceiling(x.Ratio) == -1, E);
            await CheckAsync(f, x => Math.Sqrt(x.Quantity) > 3, O);
            await CheckAsync(f, x => Math.Pow(x.Quantity, 2) == 64, E);
        }
    }
}
