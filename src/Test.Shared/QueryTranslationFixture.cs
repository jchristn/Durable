namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Shared data set for the query translation suites. Drops and recreates <c>qt_items</c> and <c>qt_owners</c>
    /// through <c>InitializeTable</c> (exercising DDL generation) and seeds a fixed set of rows containing quotes,
    /// backslashes, newlines, LIKE wildcards, non-ASCII text, NULLs, and enums stored as strings and integers.
    /// Thread safety: not thread-safe; create one per test.
    /// </summary>
    public sealed class QueryTranslationFixture : IDisposable
    {
        #region Public-Members

        /// <summary>
        /// Name of the plain ASCII item.
        /// </summary>
        public const string Alpha = "Alpha";

        /// <summary>
        /// Name of the draft item with NULL email/discount.
        /// </summary>
        public const string Beta = "Beta";

        /// <summary>
        /// Name of the item containing an apostrophe.
        /// </summary>
        public const string OBrien = "O'Brien";

        /// <summary>
        /// Name of the item containing a non-ASCII Latin character.
        /// </summary>
        public const string Zoe = "Zoë";

        /// <summary>
        /// Name of the item containing CJK characters.
        /// </summary>
        public const string Nihon = "日本";

        /// <summary>
        /// Name of the item padded with leading and trailing spaces.
        /// </summary>
        public const string Padded = "  padded  ";

        /// <summary>
        /// Gets the item repository. Never null.
        /// </summary>
        public ISqlRepository<QtItem> Items { get; }

        /// <summary>
        /// Gets the owner repository. Never null.
        /// </summary>
        public ISqlRepository<QtOwner> Owners { get; }

        /// <summary>
        /// Gets a repository reading <c>qt_items</c> with computed alias columns. Never null.
        /// </summary>
        public ISqlRepository<QtItemComputedRow> ComputedRows { get; }

        /// <summary>
        /// Gets the seeded items. Never null.
        /// </summary>
        public List<QtItem> SeededItems { get; } = new List<QtItem>();

        /// <summary>
        /// Gets the seeded owners keyed by name. Never null.
        /// </summary>
        public Dictionary<string, QtOwner> SeededOwners { get; } = new Dictionary<string, QtOwner>(StringComparer.Ordinal);

        #endregion

        #region Constructors-and-Factories

        private QueryTranslationFixture(IRepositoryProvider provider)
        {
            Items = provider.CreateRepository<QtItem>();
            Owners = provider.CreateRepository<QtOwner>();
            ComputedRows = provider.CreateRepository<QtItemComputedRow>();
        }

        /// <summary>
        /// Recreates the tables and seeds the fixed data set.
        /// </summary>
        /// <param name="provider">Repository provider. Must not be null.</param>
        /// <returns>The seeded fixture.</returns>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public static async Task<QueryTranslationFixture> CreateAsync(IRepositoryProvider provider)
        {
            ArgumentNullException.ThrowIfNull(provider);
            QueryTranslationFixture fixture = new QueryTranslationFixture(provider);
            await fixture.SeedAsync().ConfigureAwait(false);
            return fixture;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Executes the query and asserts that exactly the expected item names are returned (order-insensitive).
        /// The generated SQL is included in the failure message.
        /// </summary>
        /// <param name="query">Query to execute. Must not be null.</param>
        /// <param name="expected">Expected item names.</param>
        /// <returns>A task.</returns>
        public static async Task AssertNamesAsync(ISqlQueryBuilder<QtItem> query, params string[] expected)
        {
            ArgumentNullException.ThrowIfNull(query);
            string sql = SafeSql(query);
            List<QtItem> results;
            try
            {
                results = (await query.ExecuteAsync().ConfigureAwait(false)).ToList();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.GetType().Name + ": " + ex.Message + " | SQL: " + sql, ex);
            }

            AssertNameSet(results.Select(r => r.Name), expected, sql);
        }

        /// <summary>
        /// Asserts that the actual names equal the expected names (order-insensitive), including SQL in the failure message.
        /// </summary>
        /// <param name="actual">Actual names. Must not be null.</param>
        /// <param name="expected">Expected names. Must not be null.</param>
        /// <param name="sql">SQL for diagnostics; may be null.</param>
        public static void AssertNameSet(IEnumerable<string> actual, IEnumerable<string> expected, string? sql)
        {
            List<string> actualSorted = actual.OrderBy(n => n, StringComparer.Ordinal).ToList();
            List<string> expectedSorted = expected.OrderBy(n => n, StringComparer.Ordinal).ToList();
            bool equal = actualSorted.SequenceEqual(expectedSorted, StringComparer.Ordinal);
            Assert.True(equal,
                "Expected [" + Describe(expectedSorted) + "] but got [" + Describe(actualSorted) + "]"
                + (sql == null ? string.Empty : " | SQL: " + sql));
        }

        /// <summary>
        /// Returns the SQL of a query, or the build error text when the query cannot be built.
        /// </summary>
        /// <typeparam name="T">Row type.</typeparam>
        /// <param name="query">Query. Must not be null.</param>
        /// <returns>SQL text or error text. Never null.</returns>
        public static string SafeSql<T>(ISqlQueryBuilder<T> query) where T : class, new()
        {
            try
            {
                return query.BuildSql();
            }
            catch (Exception ex)
            {
                return "<build failed: " + ex.GetType().Name + ": " + ex.Message + ">";
            }
        }

        /// <summary>
        /// Gets the seeded item names, optionally filtered by a client-side predicate (the expected C# semantics).
        /// </summary>
        /// <param name="predicate">Client-side predicate. Must not be null.</param>
        /// <returns>Matching names. Never null.</returns>
        public string[] NamesWhere(Func<QtItem, bool> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return SeededItems.Where(predicate).Select(i => i.Name).ToArray();
        }

        /// <summary>
        /// Disposes the repositories.
        /// </summary>
        public void Dispose()
        {
            Items.Dispose();
            Owners.Dispose();
            ComputedRows.Dispose();
        }

        #endregion

        #region Private-Methods

        private static string Describe(IEnumerable<string> names)
        {
            return string.Join(", ", names.Select(n => "\"" + n.Replace("\n", "\\n", StringComparison.Ordinal) + "\""));
        }

        private async Task SeedAsync()
        {
            await Items.ExecuteSqlAsync("DROP TABLE IF EXISTS qt_items").ConfigureAwait(false);
            await Owners.ExecuteSqlAsync("DROP TABLE IF EXISTS qt_owners").ConfigureAwait(false);
            await Owners.InitializeTableAsync(typeof(QtOwner)).ConfigureAwait(false);
            await Items.InitializeTableAsync(typeof(QtItem)).ConfigureAwait(false);

            List<QtOwner> owners = new List<QtOwner>
            {
                new QtOwner { Name = "Acme", City = "Berlin" },
                new QtOwner { Name = "Globex", City = null },
                new QtOwner { Name = "Initech", City = "Austin" },
                new QtOwner { Name = "Umbrella", City = "Raccoon City" }
            };

            foreach (QtOwner owner in owners)
            {
                QtOwner created = await Owners.CreateAsync(owner).ConfigureAwait(false);
                SeededOwners[created.Name] = created;
            }

            List<QtItem> items = new List<QtItem>
            {
                new QtItem
                {
                    Name = Alpha, Code = "A-100", Email = "alpha@x.com", Status = QtStatus.Active, Priority = QtPriority.High,
                    Price = 10.50m, Ratio = 0.25, Quantity = 5, Discount = 2, IsActive = true, IsFeatured = true,
                    CreatedUtc = new DateTime(2024, 3, 15, 10, 30, 0), DueDate = new DateTime(2024, 4, 1),
                    Category = "Tools", OwnerId = SeededOwners["Acme"].Id
                },
                new QtItem
                {
                    Name = Beta, Code = "50%_off", Email = null, Status = QtStatus.Draft, Priority = QtPriority.Low,
                    Price = 20.00m, Ratio = 1.5, Quantity = 0, Discount = null, IsActive = false, IsFeatured = null,
                    CreatedUtc = new DateTime(2023, 12, 31, 23, 0, 0), DueDate = null,
                    Category = "Tools", OwnerId = SeededOwners["Acme"].Id
                },
                new QtItem
                {
                    Name = OBrien, Code = "path\\to\\file", Email = "OBRIEN@X.COM", Status = QtStatus.Suspended, Priority = QtPriority.Critical,
                    Price = 7.25m, Ratio = 2.75, Quantity = 12, Discount = 0, IsActive = true, IsFeatured = false,
                    CreatedUtc = new DateTime(2024, 1, 1, 0, 0, 0), DueDate = new DateTime(2024, 1, 10),
                    Category = "Garden", OwnerId = SeededOwners["Globex"].Id
                },
                new QtItem
                {
                    Name = Zoe, Code = "[draft]!", Email = "zoe@x.com", Status = QtStatus.Closed, Priority = QtPriority.Medium,
                    Price = 99.99m, Ratio = 0.5, Quantity = 3, Discount = 5, IsActive = true, IsFeatured = true,
                    CreatedUtc = new DateTime(2024, 2, 29, 8, 15, 0), DueDate = new DateTime(2024, 3, 5),
                    Category = "Garden", OwnerId = SeededOwners["Globex"].Id
                },
                new QtItem
                {
                    Name = Nihon, Code = "line1\nline2", Email = string.Empty, Status = QtStatus.Active, Priority = QtPriority.Medium,
                    Price = 15.00m, Ratio = -1.25, Quantity = 8, Discount = null, IsActive = false, IsFeatured = null,
                    CreatedUtc = new DateTime(2024, 7, 4, 12, 0, 0), DueDate = null,
                    Category = "Kitchen", OwnerId = null
                },
                new QtItem
                {
                    Name = Padded, Code = "100%", Email = "   ", Status = QtStatus.Active, Priority = QtPriority.Low,
                    Price = 0.10m, Ratio = 0.1, Quantity = 1, Discount = 1, IsActive = true, IsFeatured = false,
                    CreatedUtc = new DateTime(2024, 3, 16, 0, 0, 0), DueDate = new DateTime(2024, 3, 20),
                    Category = "Kitchen", OwnerId = SeededOwners["Initech"].Id
                }
            };

            foreach (QtItem item in items)
            {
                SeededItems.Add(await Items.CreateAsync(item).ConfigureAwait(false));
            }
        }

        #endregion
    }
}
