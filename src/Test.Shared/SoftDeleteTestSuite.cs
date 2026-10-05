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
    /// Soft delete coverage for a bool marker and a nullable DateTime marker: Delete, DeleteById, DeleteMany and
    /// DeleteAll set the marker instead of removing rows; soft-deleted rows are invisible to reads, counts, existence
    /// checks and Include of related collections; IgnoreQueryFilters shows them.
    /// </summary>
    public class SoftDeleteTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="SoftDeleteTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public SoftDeleteTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Bool marker: Delete, DeleteById and DeleteMany flag rows; reads and counts hide them; the rows remain.
        /// </summary>
        [Fact]
        public async Task BoolMarkerDeletesAreSoft()
        {
            ISqlRepository<RelSoftNote> repository = _Provider.CreateRepository<RelSoftNote>();
            await RelTestHelpers.RecreateTableAsync(repository);
            List<RelSoftNote> created = (await repository.CreateManyAsync(Enumerable.Range(0, 6)
                .Select(i => new RelSoftNote { ParentId = 1, Text = "n" + i }).ToList())).ToList();

            Assert.True(await repository.DeleteAsync(created[0]));
            Assert.True(repository.DeleteById(created[1].Id));
            Assert.Equal(2, await repository.DeleteManyAsync(x => x.Text == "n2" || x.Text == "n3"));

            Assert.Equal(2, await repository.CountAsync());
            Assert.Equal(2, repository.ReadAll().Count());
            Assert.Null(await repository.ReadByIdAsync(created[0].Id));
            Assert.False(await repository.ExistsByIdAsync(created[1].Id));
            Assert.False(await repository.ExistsAsync(x => x.Text == "n2"));
            Assert.Equal(new[] { "n4", "n5" }, (await repository.Query().OrderBy(x => x.Text).ExecuteAsync()).Select(x => x.Text).ToArray());

            List<RelSoftNote> everything = (await repository.Query().IgnoreQueryFilters().OrderBy(x => x.Text).ExecuteAsync()).ToList();
            Assert.Equal(6, everything.Count);
            Assert.Equal(4, everything.Count(x => x.IsDeleted));
            Assert.Equal(6L, await repository.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM rel_soft_notes"));

            Assert.Equal(2, await repository.DeleteAllAsync());
            Assert.Equal(0, await repository.CountAsync());
            Assert.Equal(6, await repository.Query().IgnoreQueryFilters().CountAsync());
        }

        /// <summary>
        /// Deleting an already soft-deleted row by id affects nothing visible; the marker is not reset.
        /// </summary>
        [Fact]
        public async Task SoftDeletedRowsAreNotUpdatedByFilteredWrites()
        {
            ISqlRepository<RelSoftNote> repository = _Provider.CreateRepository<RelSoftNote>();
            await RelTestHelpers.RecreateTableAsync(repository);
            RelSoftNote live = await repository.CreateAsync(new RelSoftNote { ParentId = 1, Text = "live" });
            RelSoftNote gone = await repository.CreateAsync(new RelSoftNote { ParentId = 1, Text = "gone" });
            await repository.DeleteAsync(gone);

            int updated = await repository.UpdateFieldAsync(x => x.ParentId == 1, x => x.Text, "touched");
            Assert.Equal(1, updated);
            RelSoftNote hidden = Assert.Single(await repository.Query().IgnoreQueryFilters().Where(x => x.Id == gone.Id).ExecuteAsync());
            Assert.Equal("gone", hidden.Text);
            Assert.True(hidden.IsDeleted);
            Assert.Equal("touched", (await repository.ReadByIdAsync(live.Id))?.Text);
        }

        /// <summary>
        /// Nullable DateTime marker: deletes stamp the time; reads hide stamped rows; IgnoreQueryFilters shows them.
        /// </summary>
        [Fact]
        public async Task TimestampMarkerDeletesAreSoft()
        {
            ISqlRepository<RelSoftStampNote> repository = _Provider.CreateRepository<RelSoftStampNote>();
            await RelTestHelpers.RecreateTableAsync(repository);
            List<RelSoftStampNote> created = (await repository.CreateManyAsync(Enumerable.Range(0, 4)
                .Select(i => new RelSoftStampNote { Text = "s" + i }).ToList())).ToList();

            DateTime before = DateTime.UtcNow.AddMinutes(-5);
            Assert.True(await repository.DeleteByIdAsync(created[0].Id));
            Assert.True(repository.Delete(created[1]));
            Assert.Equal(1, repository.DeleteMany(x => x.Text == "s2"));

            RelSoftStampNote remaining = Assert.Single(await repository.Query().ExecuteAsync());
            Assert.Equal("s3", remaining.Text);
            Assert.Null(remaining.DeletedUtc);
            Assert.Equal(1, await repository.CountAsync());

            List<RelSoftStampNote> all = (await repository.Query().IgnoreQueryFilters().ExecuteAsync()).ToList();
            Assert.Equal(4, all.Count);
            List<RelSoftStampNote> stamped = all.Where(x => x.DeletedUtc.HasValue).ToList();
            Assert.Equal(3, stamped.Count);
            Assert.All(stamped, x => Assert.True(x.DeletedUtc!.Value > before, "Deletion stamp " + x.DeletedUtc + " should be recent"));
        }

        /// <summary>
        /// Include of a collection skips soft-deleted children; a parent whose children are all soft-deleted gets an
        /// empty list.
        /// </summary>
        [Fact]
        public async Task IncludeSkipsSoftDeletedChildren()
        {
            ISqlRepository<RelSoftParent> parents = _Provider.CreateRepository<RelSoftParent>();
            ISqlRepository<RelSoftNote> notes = _Provider.CreateRepository<RelSoftNote>();
            await RelTestHelpers.RecreateTableAsync(parents);
            await RelTestHelpers.RecreateTableAsync(notes);

            RelSoftParent p1 = await parents.CreateAsync(new RelSoftParent { Name = "P1" });
            RelSoftParent p2 = await parents.CreateAsync(new RelSoftParent { Name = "P2" });
            await notes.CreateManyAsync(new List<RelSoftNote>
            {
                new RelSoftNote { ParentId = p1.Id, Text = "keep" },
                new RelSoftNote { ParentId = p1.Id, Text = "drop" },
                new RelSoftNote { ParentId = p2.Id, Text = "drop" }
            });
            Assert.Equal(2, await notes.DeleteManyAsync(x => x.Text == "drop"));

            List<RelSoftParent> loaded = (await parents.Query().Include(p => p.Notes).OrderBy(p => p.Name).ExecuteAsync()).ToList();
            Assert.Equal(2, loaded.Count);
            Assert.Equal("keep", Assert.Single(loaded[0].Notes!).Text);
            Assert.NotNull(loaded[1].Notes);
            Assert.Empty(loaded[1].Notes!);
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
