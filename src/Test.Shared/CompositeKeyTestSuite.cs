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
    /// Composite primary key coverage (two key columns ordered by <see cref="PropertyAttribute.KeyOrder"/>):
    /// DDL, Create, ReadById, Update, Delete, DeleteById, ExistsById, Upsert and key-arity validation.
    /// </summary>
    public class CompositeKeyTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="CompositeKeyTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public CompositeKeyTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Metadata orders key columns by KeyOrder, not declaration order.
        /// </summary>
        [Fact]
        public void KeyColumnsFollowKeyOrder()
        {
            ISqlRepository<RelCompositeItem> repository = _Provider.CreateRepository<RelCompositeItem>();
            Assert.True(repository.Metadata.HasCompositeKey);
            Assert.Equal(new[] { "tenant_id", "sku" }, repository.Metadata.KeyColumns.Select(c => c.Name).ToArray());
        }

        /// <summary>
        /// Create and ReadById with an object[] key; rows sharing one key part stay distinct.
        /// </summary>
        [Fact]
        public async Task CreateAndReadByCompositeKey()
        {
            ISqlRepository<RelCompositeItem> repository = await PrepareAsync();
            await repository.CreateAsync(new RelCompositeItem { TenantId = 1, Sku = "A", Name = "One-A", Quantity = 1 });
            await repository.CreateAsync(new RelCompositeItem { TenantId = 1, Sku = "B", Name = "One-B", Quantity = 2 });
            repository.Create(new RelCompositeItem { TenantId = 2, Sku = "A", Name = "Two-A", Quantity = 3 });

            RelCompositeItem? oneA = await repository.ReadByIdAsync(new object[] { 1, "A" });
            RelCompositeItem? twoA = repository.ReadById(new object[] { 2, "A" });
            RelCompositeItem? missing = await repository.ReadByIdAsync(new object[] { 2, "B" });

            Assert.NotNull(oneA);
            Assert.Equal("One-A", oneA.Name);
            Assert.NotNull(twoA);
            Assert.Equal("Two-A", twoA.Name);
            Assert.Equal(3, twoA.Quantity);
            Assert.Null(missing);
            Assert.Equal(3, await repository.CountAsync());
        }

        /// <summary>
        /// A duplicate composite key is rejected by the database.
        /// </summary>
        [Fact]
        public async Task DuplicateCompositeKeyIsRejected()
        {
            ISqlRepository<RelCompositeItem> repository = await PrepareAsync();
            await repository.CreateAsync(new RelCompositeItem { TenantId = 5, Sku = "DUP", Name = "First" });
            await Assert.ThrowsAnyAsync<Exception>(async () => await repository.CreateAsync(new RelCompositeItem { TenantId = 5, Sku = "DUP", Name = "Second" }));
        }

        /// <summary>
        /// Update targets only the row matching both key columns.
        /// </summary>
        [Fact]
        public async Task UpdateTargetsSingleCompositeRow()
        {
            ISqlRepository<RelCompositeItem> repository = await PrepareAsync();
            await repository.CreateManyAsync(new List<RelCompositeItem>
            {
                new RelCompositeItem { TenantId = 1, Sku = "X", Name = "1X", Quantity = 1 },
                new RelCompositeItem { TenantId = 2, Sku = "X", Name = "2X", Quantity = 1 },
                new RelCompositeItem { TenantId = 1, Sku = "Y", Name = "1Y", Quantity = 1 }
            });

            RelCompositeItem? item = await repository.ReadByIdAsync(new object[] { 2, "X" });
            Assert.NotNull(item);
            item.Name = "2X-updated";
            item.Quantity = 99;
            await repository.UpdateAsync(item);

            Assert.Equal("2X-updated", (await repository.ReadByIdAsync(new object[] { 2, "X" }))?.Name);
            Assert.Equal("1X", (await repository.ReadByIdAsync(new object[] { 1, "X" }))?.Name);
            Assert.Equal("1Y", (await repository.ReadByIdAsync(new object[] { 1, "Y" }))?.Name);
            Assert.Equal(1, await repository.CountAsync(x => x.Quantity == 99));
        }

        /// <summary>
        /// Delete(entity), DeleteById(object[]) and ExistsById operate on exactly one composite row.
        /// </summary>
        [Fact]
        public async Task DeleteDeleteByIdAndExistsByCompositeKey()
        {
            ISqlRepository<RelCompositeItem> repository = await PrepareAsync();
            await repository.CreateManyAsync(new List<RelCompositeItem>
            {
                new RelCompositeItem { TenantId = 1, Sku = "D1", Name = "a" },
                new RelCompositeItem { TenantId = 1, Sku = "D2", Name = "b" },
                new RelCompositeItem { TenantId = 2, Sku = "D1", Name = "c" }
            });

            Assert.True(await repository.ExistsByIdAsync(new object[] { 1, "D1" }));
            Assert.True(repository.ExistsById(new object[] { 2, "D1" }));
            Assert.False(await repository.ExistsByIdAsync(new object[] { 2, "D2" }));

            RelCompositeItem? toDelete = await repository.ReadByIdAsync(new object[] { 1, "D1" });
            Assert.NotNull(toDelete);
            Assert.True(await repository.DeleteAsync(toDelete));
            Assert.False(await repository.ExistsByIdAsync(new object[] { 1, "D1" }));
            Assert.True(await repository.ExistsByIdAsync(new object[] { 2, "D1" }));

            Assert.True(repository.DeleteById(new object[] { 2, "D1" }));
            Assert.False(await repository.DeleteByIdAsync(new object[] { 2, "D1" }));
            Assert.Equal(1, await repository.CountAsync());
            Assert.True(await repository.ExistsAsync(x => x.TenantId == 1 && x.Sku == "D2"));
        }

        /// <summary>
        /// Upsert inserts a new composite key and updates an existing one; UpsertMany mixes both.
        /// </summary>
        [Fact]
        public async Task UpsertByCompositeKey()
        {
            ISqlRepository<RelCompositeItem> repository = await PrepareAsync();

            await repository.UpsertAsync(new RelCompositeItem { TenantId = 7, Sku = "U", Name = "Inserted", Quantity = 1 });
            Assert.Equal("Inserted", (await repository.ReadByIdAsync(new object[] { 7, "U" }))?.Name);

            RelCompositeItem updated = await repository.UpsertAsync(new RelCompositeItem { TenantId = 7, Sku = "U", Name = "Updated", Quantity = 2 });
            Assert.Equal("Updated", updated.Name);
            Assert.Equal(1, await repository.CountAsync());
            Assert.Equal(2, (await repository.ReadByIdAsync(new object[] { 7, "U" }))?.Quantity);

            repository.UpsertMany(new List<RelCompositeItem>
            {
                new RelCompositeItem { TenantId = 7, Sku = "U", Name = "Again", Quantity = 3 },
                new RelCompositeItem { TenantId = 8, Sku = "U", Name = "New", Quantity = 4 }
            });

            Assert.Equal(2, await repository.CountAsync());
            Assert.Equal("Again", (await repository.ReadByIdAsync(new object[] { 7, "U" }))?.Name);
            Assert.Equal("New", (await repository.ReadByIdAsync(new object[] { 8, "U" }))?.Name);
        }

        /// <summary>
        /// Supplying the wrong number of key values (or a scalar) for a composite key throws ArgumentException.
        /// </summary>
        [Fact]
        public async Task WrongKeyArityThrowsArgumentException()
        {
            ISqlRepository<RelCompositeItem> repository = await PrepareAsync();

            await Assert.ThrowsAsync<ArgumentException>(async () => await repository.ReadByIdAsync(new object[] { 1 }));
            await Assert.ThrowsAsync<ArgumentException>(async () => await repository.ReadByIdAsync(new object[] { 1, "A", "extra" }));
            await Assert.ThrowsAsync<ArgumentException>(async () => await repository.ReadByIdAsync(1));
            await Assert.ThrowsAsync<ArgumentException>(async () => await repository.ReadByIdAsync("A"));
            Assert.Throws<ArgumentException>(() => repository.DeleteById(new object[] { 1 }));
            Assert.Throws<ArgumentException>(() => repository.ExistsById(new object[] { 1, "A", 3 }));
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<RelCompositeItem>> PrepareAsync()
        {
            ISqlRepository<RelCompositeItem> repository = _Provider.CreateRepository<RelCompositeItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            return repository;
        }

        #endregion
    }
}
