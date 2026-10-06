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
    /// Version column semantics on every provider: a bare <c>[VersionColumn]</c> infers its type from the property type,
    /// <see cref="VersionColumnType.BinaryCounter"/> is an 8-byte counter Durable maintains (starting at 1, incremented
    /// per update), stale updates fail, and set-based updates make held copies stale. Mismatched declarations fail when the
    /// metadata is built.
    /// </summary>
    public class VersionColumnTestSuite
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the suite.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public VersionColumnTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// A bare [VersionColumn] on byte[] resolves to BinaryCounter; on int to Integer.
        /// </summary>
        [Fact]
        public void BareAttributeInfersTypeFromProperty()
        {
            Assert.Equal(VersionColumnType.BinaryCounter, EntityMetadata.For(typeof(RelBinaryVersionedItem)).VersionInfo!.Type);
            Assert.Equal(VersionColumnType.Integer, EntityMetadata.For(typeof(RelVersionedItem)).VersionInfo!.Type);
            Assert.Null(new VersionColumnAttribute().Type);
            Assert.Equal(VersionColumnType.Guid, new VersionColumnAttribute(VersionColumnType.Guid).Type);
        }

        /// <summary>
        /// A declared type that does not suit the property type is rejected with a clear message.
        /// </summary>
        [Fact]
        public void MismatchedDeclarationIsRejected()
        {
            System.Reflection.PropertyInfo property = typeof(RelVersionedItem).GetProperty(nameof(RelVersionedItem.Version))!;
            InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => new VersionColumnInfo("version", property, VersionColumnType.BinaryCounter));
            Assert.Contains("BinaryCounter", error.Message);
            Assert.Throws<InvalidOperationException>(() => new VersionColumnInfo("name", typeof(RelVersionedItem).GetProperty(nameof(RelVersionedItem.Name))!, null));
        }

        /// <summary>
        /// BinaryCounter starts at the 8-byte value 1, each update increments it by one, the stored value round-trips,
        /// and an update from a stale copy throws.
        /// </summary>
        [Fact]
        public async Task BinaryCounterIncrementsAndDetectsStaleUpdates()
        {
            ISqlRepository<RelBinaryVersionedItem> repository = _Provider.CreateRepository<RelBinaryVersionedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            RelBinaryVersionedItem created = await repository.CreateAsync(new RelBinaryVersionedItem { Name = "a", Quantity = 1 });
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }, created.RowVersion);

            RelBinaryVersionedItem stale = (await repository.ReadByIdAsync(created.Id))!;
            Assert.Equal(created.RowVersion, stale.RowVersion);

            created.Quantity = 2;
            RelBinaryVersionedItem updated = await repository.UpdateAsync(created);
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 2 }, updated.RowVersion);
            RelBinaryVersionedItem stored = (await repository.ReadByIdAsync(created.Id))!;
            Assert.Equal(updated.RowVersion, stored.RowVersion);
            Assert.Equal(2, stored.Quantity);

            stale.Quantity = 99;
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(() => repository.UpdateAsync(stale));
            stale.Quantity = 98;
            Assert.Throws<OptimisticConcurrencyException>(() => repository.Update(stale));
            Assert.Equal(2, (await repository.ReadByIdAsync(created.Id))!.Quantity);

            stored.Name = "b";
            repository.Update(stored);
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 3 }, (await repository.ReadByIdAsync(created.Id))!.RowVersion);
        }

        /// <summary>
        /// UpdateField and BatchUpdate replace a BinaryCounter version, so copies read before them are stale; the new
        /// value is larger than the old one.
        /// </summary>
        [Fact]
        public async Task SetBasedUpdatesReplaceBinaryCounter()
        {
            ISqlRepository<RelBinaryVersionedItem> repository = _Provider.CreateRepository<RelBinaryVersionedItem>();
            await RelTestHelpers.RecreateTableAsync(repository);

            List<RelBinaryVersionedItem> created = (await repository.CreateManyAsync(new List<RelBinaryVersionedItem>
            {
                new RelBinaryVersionedItem { Name = "a", Quantity = 1 },
                new RelBinaryVersionedItem { Name = "b", Quantity = 2 }
            })).ToList();

            RelBinaryVersionedItem heldA = (await repository.ReadByIdAsync(created[0].Id))!;
            RelBinaryVersionedItem heldB = (await repository.ReadByIdAsync(created[1].Id))!;

            Assert.Equal(1, await repository.UpdateFieldAsync(x => x.Id == heldA.Id, x => x.Quantity, 10));
            byte[] afterField = (await repository.ReadByIdAsync(heldA.Id))!.RowVersion!;
            Assert.NotEqual(heldA.RowVersion, afterField);
            Assert.True(CompareBigEndian(afterField, heldA.RowVersion!) > 0);
            heldA.Name = "stale";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(() => repository.UpdateAsync(heldA));

            Assert.Equal(2, await repository.BatchUpdateAsync(x => x.Quantity > 0, x => new RelBinaryVersionedItem { Quantity = x.Quantity + 1 }));
            heldB.Name = "stale";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(() => repository.UpdateAsync(heldB));

            RelBinaryVersionedItem fresh = (await repository.ReadByIdAsync(heldB.Id))!;
            fresh.Name = "fresh";
            RelBinaryVersionedItem saved = await repository.UpdateAsync(fresh);
            Assert.Equal("fresh", (await repository.ReadByIdAsync(heldB.Id))!.Name);
            Assert.True(CompareBigEndian(saved.RowVersion!, afterField) > 0);
        }

        #endregion

        #region Private-Methods

        private static int CompareBigEndian(byte[] left, byte[] right)
        {
            if (left.Length != right.Length) return left.Length.CompareTo(right.Length);
            for (int i = 0; i < left.Length; i++)
            {
                if (left[i] != right[i]) return left[i].CompareTo(right[i]);
            }

            return 0;
        }

        #endregion
    }
}
