namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// The shared <see cref="CfItem"/> / <see cref="CfOwner"/> data set. Values avoid collation-dependent cases (no two
    /// names differ only by case or accent, no trailing-space equality) and use binary-exact decimals, so every backend in
    /// its default string mode must return exactly the rows a C# LINQ-to-objects evaluation returns.
    /// </summary>
    internal sealed class ItemFixture
    {
        internal const string Alpha = "Alpha";
        internal const string Beta = "Beta";
        internal const string OBrien = "O'Brien";
        internal const string Delta = "Delta";
        internal const string Echo = "Echo";
        internal const string Foxtrot = "Foxtrot";

        internal static readonly string[] AllNames = new[] { Alpha, Beta, OBrien, Delta, Echo, Foxtrot };

        private ItemFixture(IRepository<CfItem> items, IRepository<CfOwner> owners)
        {
            Items = items;
            Owners = owners;
        }

        internal IRepository<CfItem> Items { get; }

        internal IRepository<CfOwner> Owners { get; }

        internal List<CfItem> SeededItems { get; } = new List<CfItem>();

        internal Dictionary<string, CfOwner> SeededOwners { get; } = new Dictionary<string, CfOwner>(StringComparer.Ordinal);

        internal static async Task<ItemFixture> CreateAsync(IRepository<CfItem> items, IRepository<CfOwner> owners, CancellationToken token)
        {
            ItemFixture fixture = new ItemFixture(items, owners);
            await fixture.SeedAsync(token).ConfigureAwait(false);
            return fixture;
        }

        internal CfItem Item(string name)
        {
            CfItem? item = SeededItems.FirstOrDefault(i => i.Name == name);
            if (item == null) throw new InvalidOperationException("Item " + name + " was not seeded.");
            return item;
        }

        internal string[] NamesWhere(Func<CfItem, bool> predicate)
        {
            return SeededItems.Where(predicate).Select(i => i.Name).ToArray();
        }

        /// <summary>
        /// Checks the expectation against a client-side evaluation of the predicate (when it can be evaluated in memory),
        /// then runs it through Query().Where() and compares the returned names.
        /// </summary>
        internal async Task CheckAsync(Expression<Func<CfItem, bool>> predicate, string[] expected, CancellationToken token)
        {
            VerifyOracle(predicate, expected);
            List<CfItem> rows;
            try
            {
                rows = (await Items.Query().Where(predicate).ExecuteAsync(token).ConfigureAwait(false)).ToList();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Query().Where(" + predicate + ") failed: " + ex.GetType().Name + ": " + ex.Message, ex);
            }

            ConformanceAssert.NameSet(rows.Select(r => r.Name), expected, "Where(" + predicate + ")");
        }

        internal void VerifyOracle(Expression<Func<CfItem, bool>> predicate, string[] expected)
        {
            Func<CfItem, bool> compiled = predicate.Compile();
            string[] clientNames;
            try
            {
                clientNames = NamesWhere(compiled);
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

            ConformanceAssert.NameSet(clientNames, expected, "Test expectation disagrees with LINQ-to-objects for " + predicate);
        }

        private async Task SeedAsync(CancellationToken token)
        {
            List<CfOwner> owners = new List<CfOwner>
            {
                new CfOwner { Name = "Acme", City = "Berlin" },
                new CfOwner { Name = "Globex", City = null },
                new CfOwner { Name = "Initech", City = "Austin" },
                new CfOwner { Name = "Umbrella", City = "Raccoon City" }
            };

            foreach (CfOwner owner in owners)
            {
                CfOwner created = await Owners.CreateAsync(owner, null, token).ConfigureAwait(false);
                SeededOwners[created.Name] = created;
            }

            List<CfItem> items = new List<CfItem>
            {
                new CfItem
                {
                    Name = Alpha, Code = "A-100", Email = "alpha@x.com", Status = CfStatus.Active, Priority = CfPriority.High,
                    Price = 10.50m, Ratio = 0.25, Quantity = 5, Discount = 2, BigNumber = 5000000000L, IsActive = true, IsFeatured = true,
                    CreatedUtc = new DateTime(2024, 3, 15, 10, 30, 0), DueDate = new DateTime(2024, 4, 1),
                    Token = new Guid("11111111-1111-1111-1111-111111111111"), Category = "Tools", OwnerId = SeededOwners["Acme"].Id
                },
                new CfItem
                {
                    Name = Beta, Code = "50%_off", Email = null, Status = CfStatus.Draft, Priority = CfPriority.Low,
                    Price = 20.00m, Ratio = 1.5, Quantity = 0, Discount = null, BigNumber = -7L, IsActive = false, IsFeatured = null,
                    CreatedUtc = new DateTime(2023, 12, 31, 23, 0, 0), DueDate = null,
                    Token = new Guid("22222222-2222-2222-2222-222222222222"), Category = "Tools", OwnerId = SeededOwners["Acme"].Id
                },
                new CfItem
                {
                    Name = OBrien, Code = "path\\to\\file", Email = "obrien@x.com", Status = CfStatus.Suspended, Priority = CfPriority.Critical,
                    Price = 7.25m, Ratio = 2.75, Quantity = 12, Discount = 0, BigNumber = 0L, IsActive = true, IsFeatured = false,
                    CreatedUtc = new DateTime(2024, 1, 1, 0, 0, 0), DueDate = new DateTime(2024, 1, 10),
                    Token = new Guid("33333333-3333-3333-3333-333333333333"), Category = "Garden", OwnerId = SeededOwners["Globex"].Id
                },
                new CfItem
                {
                    Name = Delta, Code = "[draft]!", Email = "delta@x.com", Status = CfStatus.Closed, Priority = CfPriority.Medium,
                    Price = 99.75m, Ratio = 0.5, Quantity = 3, Discount = 5, BigNumber = 42L, IsActive = true, IsFeatured = true,
                    CreatedUtc = new DateTime(2024, 2, 29, 8, 15, 45), DueDate = new DateTime(2024, 3, 5),
                    Token = new Guid("44444444-4444-4444-4444-444444444444"), Category = "Garden", OwnerId = SeededOwners["Globex"].Id
                },
                new CfItem
                {
                    Name = Echo, Code = "line1\nline2", Email = string.Empty, Status = CfStatus.Active, Priority = CfPriority.Medium,
                    Price = 15.00m, Ratio = -1.25, Quantity = 8, Discount = null, BigNumber = 9000000000L, IsActive = false, IsFeatured = null,
                    CreatedUtc = new DateTime(2024, 7, 4, 12, 0, 0), DueDate = null,
                    Token = new Guid("55555555-5555-5555-5555-555555555555"), Category = "Kitchen", OwnerId = null
                },
                new CfItem
                {
                    Name = Foxtrot, Code = "  100%  ", Email = "   ", Status = CfStatus.Active, Priority = CfPriority.Low,
                    Price = 0.50m, Ratio = 0.125, Quantity = 1, Discount = 1, BigNumber = 1L, IsActive = true, IsFeatured = false,
                    CreatedUtc = new DateTime(2024, 3, 16, 0, 0, 0), DueDate = new DateTime(2024, 3, 20),
                    Token = new Guid("66666666-6666-6666-6666-666666666666"), Category = "Kitchen", OwnerId = SeededOwners["Initech"].Id
                }
            };

            foreach (CfItem item in items)
            {
                SeededItems.Add(await Items.CreateAsync(item, null, token).ConfigureAwait(false));
            }
        }
    }
}
