namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// <see cref="ValueConverterAttribute"/> coverage: a struct stored as cents, a list stored as CSV, and an enum
    /// stored as custom codes. Verifies the stored representation, round-trips, and that converters are applied to
    /// parameter values in predicates and bulk updates.
    /// </summary>
    public class ValueConverterTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ValueConverterTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public ValueConverterTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Converted values round-trip and are stored in their provider representation.
        /// </summary>
        [Fact]
        public async Task ConvertedValuesRoundTripAndAreStoredConverted()
        {
            ISqlRepository<RelConvertedItem> repository = await PrepareAsync();
            RelConvertedItem created = await repository.CreateAsync(new RelConvertedItem
            {
                Name = "Widget",
                Price = new Money(1999),
                Tags = new List<string> { "red", "large", "sale" },
                Priority = RelPriority.High
            });

            RelConvertedItem? loaded = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(loaded);
            Assert.Equal(new Money(1999), loaded.Price);
            Assert.Equal(new[] { "red", "large", "sale" }, loaded.Tags.ToArray());
            Assert.Equal(RelPriority.High, loaded.Priority);

            long cents = await repository.ExecuteScalarAsync<long>("SELECT price FROM rel_converted_items WHERE id = @p0", null, default, created.Id);
            string? tags = await repository.ExecuteScalarAsync<string>("SELECT tags FROM rel_converted_items WHERE id = @p0", null, default, created.Id);
            string? priority = await repository.ExecuteScalarAsync<string>("SELECT priority FROM rel_converted_items WHERE id = @p0", null, default, created.Id);
            Assert.Equal(1999L, cents);
            Assert.Equal("red,large,sale", tags);
            Assert.Equal("H", priority?.Trim());
        }

        /// <summary>
        /// Converted values survive CreateMany and Update.
        /// </summary>
        [Fact]
        public async Task ConvertedValuesSurviveCreateManyAndUpdate()
        {
            ISqlRepository<RelConvertedItem> repository = await PrepareAsync();
            List<RelConvertedItem> created = (await repository.CreateManyAsync(new List<RelConvertedItem>
            {
                new RelConvertedItem { Name = "a", Price = new Money(100), Tags = new List<string> { "x" }, Priority = RelPriority.Low },
                new RelConvertedItem { Name = "b", Price = new Money(200), Tags = new List<string>(), Priority = RelPriority.Medium }
            })).ToList();

            RelConvertedItem b = Assert.Single(await repository.Query().Where(x => x.Name == "b").ExecuteAsync());
            Assert.Empty(b.Tags);
            Assert.Equal(RelPriority.Medium, b.Priority);

            b.Price = new Money(250);
            b.Tags = new List<string> { "y", "z" };
            b.Priority = RelPriority.High;
            await repository.UpdateAsync(b);

            RelConvertedItem? reloaded = await repository.ReadByIdAsync(created[1].Id);
            Assert.NotNull(reloaded);
            Assert.Equal(new Money(250), reloaded.Price);
            Assert.Equal(new[] { "y", "z" }, reloaded.Tags.ToArray());
            Assert.Equal(RelPriority.High, reloaded.Priority);
        }

        /// <summary>
        /// A predicate comparing a converted enum property to a constant converts the parameter (High -> "H").
        /// </summary>
        [Fact]
        public async Task WherePredicateOnConvertedEnumAppliesConverter()
        {
            ISqlRepository<RelConvertedItem> repository = await SeedAsync();
            repository.CaptureSql = true;

            List<RelConvertedItem> high = (await repository.Query().Where(x => x.Priority == RelPriority.High).ExecuteAsync()).ToList();
            string? sql = repository.LastExecutedSqlWithParameters;

            Assert.True(high.Count == 2, "Expected 2 high-priority rows, got " + high.Count + ". SQL: " + sql);
            Assert.All(high, x => Assert.Equal(RelPriority.High, x.Priority));

            RelPriority wanted = RelPriority.Low;
            Assert.Equal(1, await repository.CountAsync(x => x.Priority == wanted));
            Assert.Equal(3, await repository.CountAsync(x => x.Priority != RelPriority.Low));
        }

        /// <summary>
        /// A predicate comparing a converted struct property to a value converts the parameter (Money -> cents).
        /// </summary>
        [Fact]
        public async Task WherePredicateOnConvertedStructAppliesConverter()
        {
            ISqlRepository<RelConvertedItem> repository = await SeedAsync();
            repository.CaptureSql = true;

            Money price = new Money(500);
            List<RelConvertedItem> matches = (await repository.Query().Where(x => x.Price == price).ExecuteAsync()).ToList();
            string? sql = repository.LastExecutedSqlWithParameters;

            Assert.True(matches.Count == 2, "Expected 2 rows priced 500c, got " + matches.Count + ". SQL: " + sql);
            Assert.True(await repository.ExistsAsync(x => x.Price == new Money(1500)));
            Assert.False(await repository.ExistsAsync(x => x.Price == new Money(1)));
        }

        /// <summary>
        /// A predicate on a CSV-converted list compared to a list value converts the parameter.
        /// </summary>
        [Fact]
        public async Task WherePredicateOnConvertedListAppliesConverter()
        {
            ISqlRepository<RelConvertedItem> repository = await SeedAsync();
            List<string> tags = new List<string> { "a", "b" };
            RelConvertedItem match = Assert.Single(await repository.Query().Where(x => x.Tags == tags).ExecuteAsync());
            Assert.Equal("one", match.Name);
        }

        /// <summary>
        /// UpdateField and BatchUpdate convert the assigned value.
        /// </summary>
        [Fact]
        public async Task UpdateFieldAndBatchUpdateApplyConverter()
        {
            ISqlRepository<RelConvertedItem> repository = await SeedAsync();

            int fieldRows = await repository.UpdateFieldAsync(x => x.Name == "one", x => x.Priority, RelPriority.Low);
            Assert.Equal(1, fieldRows);
            string? stored = await repository.ExecuteScalarAsync<string>("SELECT priority FROM rel_converted_items WHERE name = @p0", null, default, "one");
            Assert.Equal("L", stored?.Trim());

            int batchRows = await repository.BatchUpdateAsync(x => x.Name == "two", x => new RelConvertedItem { Price = new Money(42), Priority = RelPriority.Medium });
            Assert.Equal(1, batchRows);
            RelConvertedItem two = Assert.Single(await repository.Query().Where(x => x.Name == "two").ExecuteAsync());
            Assert.Equal(new Money(42), two.Price);
            Assert.Equal(RelPriority.Medium, two.Priority);
            long cents = await repository.ExecuteScalarAsync<long>("SELECT price FROM rel_converted_items WHERE name = @p0", null, default, "two");
            Assert.Equal(42L, cents);
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<RelConvertedItem>> PrepareAsync()
        {
            ISqlRepository<RelConvertedItem> repository = _Provider.CreateRepository<RelConvertedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            return repository;
        }

        private async Task<ISqlRepository<RelConvertedItem>> SeedAsync()
        {
            ISqlRepository<RelConvertedItem> repository = await PrepareAsync();
            await repository.CreateManyAsync(new List<RelConvertedItem>
            {
                new RelConvertedItem { Name = "one", Price = new Money(500), Tags = new List<string> { "a", "b" }, Priority = RelPriority.High },
                new RelConvertedItem { Name = "two", Price = new Money(500), Tags = new List<string> { "c" }, Priority = RelPriority.Medium },
                new RelConvertedItem { Name = "three", Price = new Money(1500), Tags = new List<string>(), Priority = RelPriority.High },
                new RelConvertedItem { Name = "four", Price = new Money(2500), Tags = new List<string> { "a" }, Priority = RelPriority.Low }
            });
            return repository;
        }

        #endregion
    }
}
