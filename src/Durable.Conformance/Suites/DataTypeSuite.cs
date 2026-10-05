namespace Durable.Conformance
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Every scalar type round-trips exactly: strings (unicode, special characters, empty vs null), int, nullable int,
    /// long, decimal, double, bool, nullable bool, DateTime, nullable DateTime, Guid and both enum storage modes.
    /// </summary>
    internal sealed class DataTypeSuite : KitSuite
    {
        public DataTypeSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Every column of a fully populated entity round-trips")]
        public async Task AllScalarTypesRoundTrip()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            Guid token = Guid.NewGuid();
            CfItem created = await repository.CreateAsync(new CfItem
            {
                Name = "Full", Code = "C-1", Email = "full@x.com", Status = CfStatus.Suspended, Priority = CfPriority.Critical,
                Price = 1234567.25m, Ratio = -98765.4375, Quantity = int.MaxValue, Discount = int.MinValue, BigNumber = long.MaxValue,
                IsActive = true, IsFeatured = false, CreatedUtc = new DateTime(2024, 2, 29, 23, 59, 58), DueDate = new DateTime(1999, 12, 31),
                Token = token, Category = "Types", OwnerId = 42
            }, null, Token);

            CfItem? stored = await repository.ReadByIdAsync(created.Id, null, Token);
            Assert.NotNull(stored);
            Assert.Equal("Full", stored.Name);
            Assert.Equal("C-1", stored.Code);
            Assert.Equal("full@x.com", stored.Email);
            Assert.Equal(CfStatus.Suspended, stored.Status);
            Assert.Equal(CfPriority.Critical, stored.Priority);
            Assert.Equal(1234567.25m, stored.Price);
            Assert.Equal(-98765.4375, stored.Ratio);
            Assert.Equal(int.MaxValue, stored.Quantity);
            Assert.Equal(int.MinValue, stored.Discount);
            Assert.Equal(long.MaxValue, stored.BigNumber);
            Assert.True(stored.IsActive);
            Assert.False(stored.IsFeatured);
            ConformanceAssert.SameInstant(new DateTime(2024, 2, 29, 23, 59, 58), stored.CreatedUtc, "CreatedUtc");
            ConformanceAssert.SameInstant(new DateTime(1999, 12, 31), stored.DueDate!.Value, "DueDate");
            Assert.Equal(token, stored.Token);
            Assert.Equal("Types", stored.Category);
            Assert.Equal(42, stored.OwnerId);
        }

        [ConformanceTest(Description = "Nullable columns round-trip null and update back to values")]
        public async Task NullableColumnsRoundTripNull()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            CfItem created = repository.Create(new CfItem { Name = "Nulls", Category = "c", CreatedUtc = new DateTime(2020, 1, 1) });
            CfItem? stored = repository.ReadById(created.Id);
            Assert.NotNull(stored);
            Assert.Null(stored.Code);
            Assert.Null(stored.Email);
            Assert.Null(stored.Discount);
            Assert.Null(stored.IsFeatured);
            Assert.Null(stored.DueDate);
            Assert.Null(stored.OwnerId);

            stored.Discount = 0;
            stored.IsFeatured = true;
            stored.DueDate = new DateTime(2021, 6, 7);
            stored.Email = string.Empty;
            repository.Update(stored);
            CfItem? updated = repository.ReadById(created.Id);
            Assert.NotNull(updated);
            Assert.Equal(0, updated.Discount);
            Assert.True(updated.IsFeatured);
            Assert.Equal(string.Empty, updated.Email);
            ConformanceAssert.SameInstant(new DateTime(2021, 6, 7), updated.DueDate!.Value, "DueDate");
        }

        [ConformanceTest(Description = "Unicode, quotes, backslashes, control characters and empty strings round-trip exactly")]
        public async Task StringsRoundTripExactly()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            string[] values = new[]
            {
                "Zoë", "日本語", "O'Brien", "say \"hi\"", "back\\slash", "line1\nline2", "tab\there", "emoji \U0001F600", "50%_[x]", string.Empty
            };

            foreach (string value in values)
            {
                CfItem created = await repository.CreateAsync(new CfItem { Name = value, Code = value, Category = "s" }, null, Token);
                CfItem? stored = await repository.ReadByIdAsync(created.Id, null, Token);
                Assert.NotNull(stored);
                Assert.True(value == stored.Name, "Name should round-trip \"" + value + "\" but was \"" + stored.Name + "\"");
                Assert.True(value == stored.Code, "Code should round-trip \"" + value + "\" (not null) but was " + (stored.Code == null ? "null" : "\"" + stored.Code + "\""));
            }

            Assert.Equal((long)values.Length, await repository.CountAsync(null, null, Token));
        }

        [ConformanceTest(Description = "Every enum value round-trips for name storage and integer storage")]
        public async Task EnumsRoundTrip()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            foreach (CfStatus status in Enum.GetValues<CfStatus>())
            {
                foreach (CfPriority priority in Enum.GetValues<CfPriority>())
                {
                    CfItem created = repository.Create(new CfItem { Name = status + "-" + priority, Category = "e", Status = status, Priority = priority });
                    CfItem? stored = repository.ReadById(created.Id);
                    Assert.NotNull(stored);
                    Assert.Equal(status, stored.Status);
                    Assert.Equal(priority, stored.Priority);
                }
            }

            Assert.Equal(4L, repository.Count(x => x.Status == CfStatus.Closed));
            Assert.Equal(4L, repository.Count(x => x.Priority == CfPriority.Low));
        }

        [ConformanceTest(Description = "Boundary dates, Guid.Empty and zero values round-trip")]
        public async Task BoundaryValuesRoundTrip()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            CfItem early = repository.Create(new CfItem
            {
                Name = "early", Category = "b", CreatedUtc = new DateTime(1900, 1, 1), DueDate = new DateTime(2999, 12, 31, 23, 59, 59),
                Token = Guid.Empty, Price = 0m, Ratio = 0d, BigNumber = long.MinValue, Quantity = 0
            });
            CfItem negative = await repository.CreateAsync(new CfItem { Name = "negative", Category = "b", Price = -0.75m, Ratio = -0.5, Quantity = -3 }, null, Token);

            CfItem? storedEarly = repository.ReadById(early.Id);
            Assert.NotNull(storedEarly);
            ConformanceAssert.SameInstant(new DateTime(1900, 1, 1), storedEarly.CreatedUtc, "CreatedUtc 1900-01-01");
            ConformanceAssert.SameInstant(new DateTime(2999, 12, 31, 23, 59, 59), storedEarly.DueDate!.Value, "DueDate 2999-12-31");
            Assert.Equal(Guid.Empty, storedEarly.Token);
            Assert.Equal(0m, storedEarly.Price);
            Assert.Equal(long.MinValue, storedEarly.BigNumber);

            CfItem? storedNegative = await repository.ReadByIdAsync(negative.Id, null, Token);
            Assert.NotNull(storedNegative);
            Assert.Equal(-0.75m, storedNegative.Price);
            Assert.Equal(-0.5, storedNegative.Ratio);
            Assert.Equal(-3, storedNegative.Quantity);
        }

        [ConformanceTest(Description = "Values bound in predicates match stored values exactly for every scalar type")]
        public async Task StoredValuesMatchPredicates()
        {
            await ResetAsync(typeof(CfItem));
            IRepository<CfItem> repository = Repository<CfItem>();
            Guid token = Guid.NewGuid();
            DateTime created = new DateTime(2022, 8, 9, 10, 11, 12);
            CfItem item = repository.Create(new CfItem
            {
                Name = "Match", Category = "m", Price = 19.75m, Ratio = 0.375, BigNumber = 8000000000L, Token = token, CreatedUtc = created,
                IsFeatured = false, Status = CfStatus.Draft, Priority = CfPriority.High
            });
            repository.Create(new CfItem { Name = "Other", Category = "m", Price = 1m, Ratio = 1, BigNumber = 1, Token = Guid.NewGuid(), CreatedUtc = created.AddDays(1) });

            Assert.Equal(item.Id, repository.ReadSingle(x => x.Price == 19.75m).Id);
            Assert.Equal(item.Id, repository.ReadSingle(x => x.Ratio == 0.375).Id);
            Assert.Equal(item.Id, repository.ReadSingle(x => x.BigNumber == 8000000000L).Id);
            Assert.Equal(item.Id, repository.ReadSingle(x => x.Token == token).Id);
            Assert.Equal(item.Id, repository.ReadSingle(x => x.CreatedUtc == created).Id);
            Assert.Equal(item.Id, repository.ReadSingle(x => x.IsFeatured == false).Id);
            Assert.Equal(item.Id, repository.ReadSingle(x => x.Status == CfStatus.Draft && x.Priority == CfPriority.High).Id);
            Assert.Equal(item.Id, (await repository.ReadSingleAsync(x => x.Name == "Match" && x.Category == "m", null, Token)).Id);
        }
    }
}
