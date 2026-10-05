namespace Durable.Conformance
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Soft delete (always required): every delete path marks rows instead of removing them, marked rows are invisible to
    /// reads, counts, existence checks, aggregates and predicate-based writes, and IgnoreQueryFilters shows them again.
    /// </summary>
    internal sealed class SoftDeleteSuite : KitSuite
    {
        public SoftDeleteSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Delete / DeleteById / DeleteMany mark rows; reads no longer see them")]
        public async Task DeletesAreSoft()
        {
            IRepository<CfNote> repository = Repository<CfNote>();
            List<CfNote> created = await SeedAsync(repository, 6);
            Assert.True(repository.Delete(created[0]));
            Assert.True(repository.DeleteById(created[1].Id));
            Assert.Equal(2, repository.DeleteMany(x => x.Text == "n2" || x.Text == "n3"));

            Assert.Equal(2L, repository.Count());
            Assert.Equal(2, repository.ReadAll().Count());
            Assert.Null(repository.ReadById(created[0].Id));
            Assert.False(repository.ExistsById(created[1].Id));
            Assert.False(repository.Exists(x => x.Text == "n2"));
            Assert.Null(repository.ReadFirst(x => x.Text == "n3"));
            ConformanceAssert.Sequence(repository.Query().OrderBy(x => x.Text).Execute().Select(x => x.Text), new[] { "n4", "n5" }, "visible notes");

            List<CfNote> everything = repository.Query().IgnoreQueryFilters().OrderBy(x => x.Text).Execute().ToList();
            Assert.Equal(6, everything.Count);
            Assert.Equal(new[] { "n0", "n1", "n2", "n3" }, everything.Where(x => x.IsDeleted).Select(x => x.Text).ToArray());
        }

        [ConformanceTest(Description = "Async delete paths are soft")]
        public async Task AsyncDeletesAreSoft()
        {
            IRepository<CfNote> repository = Repository<CfNote>();
            List<CfNote> created = await SeedAsync(repository, 5);
            Assert.True(await repository.DeleteAsync(created[0], null, Token));
            Assert.True(await repository.DeleteByIdAsync(created[1].Id, null, Token));
            Assert.Equal(1, await repository.DeleteManyAsync(x => x.Text == "n2", null, Token));
            Assert.Equal(1, await repository.Query().Where(x => x.Text == "n3").DeleteAsync(Token));
            Assert.Equal(1L, await repository.CountAsync(null, null, Token));
            Assert.Equal(5L, await repository.Query().IgnoreQueryFilters().CountAsync(Token));
        }

        [ConformanceTest(Description = "DeleteAll, Query().Delete() and BatchDelete are soft and only count live rows")]
        public async Task BulkDeletesAreSoft()
        {
            IRepository<CfNote> repository = Repository<CfNote>();
            await SeedAsync(repository, 6);
            Assert.Equal(2, repository.BatchDelete(x => x.Text == "n0" || x.Text == "n1"));
            Assert.Equal(1, repository.Query().Where(x => x.Text == "n2").Delete());
            Assert.Equal(1, await repository.BatchDeleteAsync(x => x.Text == "n2" || x.Text == "n3", null, Token));
            Assert.Equal(2, repository.DeleteAll());
            Assert.Equal(0, await repository.DeleteAllAsync(null, Token));
            Assert.Equal(0L, repository.Count());
            Assert.Equal(6L, repository.Query().IgnoreQueryFilters().Count());
            Assert.All(repository.Query().IgnoreQueryFilters().Execute(), x => Assert.True(x.IsDeleted));
        }

        [ConformanceTest(Description = "UpdateMany does not touch soft-deleted rows")]
        public async Task UpdateManySkipsDeletedRows()
        {
            IRepository<CfNote> repository = Repository<CfNote>();
            List<CfNote> created = await SeedAsync(repository, 2);
            repository.Delete(created[1]);
            Assert.Equal(1, repository.UpdateMany(x => x.AuthorId == 1, note => note.Text = "touched"));
            CfNote hidden = Assert.Single(repository.Query().IgnoreQueryFilters().Where(x => x.Id == created[1].Id).Execute());
            Assert.Equal("n1", hidden.Text);
            Assert.True(hidden.IsDeleted);
            Assert.Equal("touched", repository.ReadById(created[0].Id)?.Text);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.BatchUpdate, Description = "UpdateField and BatchUpdate do not touch soft-deleted rows")]
        public async Task SetBasedUpdatesSkipDeletedRows()
        {
            IRepository<CfNote> repository = Repository<CfNote>();
            List<CfNote> created = await SeedAsync(repository, 3);
            await repository.DeleteAsync(created[2], null, Token);
            Assert.Equal(2, await repository.UpdateFieldAsync(x => x.AuthorId == 1, x => x.Text, "field", null, Token));
            Assert.Equal(2, repository.BatchUpdate(x => x.AuthorId == 1, x => new CfNote { AuthorId = 2 }));
            CfNote hidden = Assert.Single(repository.Query().IgnoreQueryFilters().Where(x => x.Id == created[2].Id).Execute());
            Assert.Equal("n2", hidden.Text);
            Assert.Equal(1, hidden.AuthorId);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.Aggregates, Description = "Soft-deleted rows are excluded from aggregates")]
        public async Task AggregatesSkipDeletedRows()
        {
            IRepository<CfNote> repository = Repository<CfNote>();
            List<CfNote> created = await SeedAsync(repository, 4);
            repository.UpdateMany(x => x.Text == "n3", note => note.AuthorId = 50);
            Assert.Equal(53m, repository.Sum(x => x.AuthorId));
            repository.Delete(created[3]);
            Assert.Equal(3m, repository.Sum(x => x.AuthorId));
            Assert.Equal(1, repository.Max(x => x.AuthorId));
        }

        [ConformanceTest(Description = "Soft delete combines with query filters; IgnoreQueryFilters bypasses both")]
        public async Task SoftDeleteWithQueryFilter()
        {
            IRepository<CfNote> repository = Repository<CfNote>();
            List<CfNote> created = await SeedAsync(repository, 4);
            repository.UpdateMany(x => x.Text == "n2" || x.Text == "n3", note => note.AuthorId = 2);
            repository.Delete(created[0]);
            repository.AddQueryFilter(x => x.AuthorId == 1);
            Assert.Equal(new[] { "n1" }, repository.ReadAll().Select(x => x.Text).ToArray());
            Assert.Equal(4L, repository.Query().IgnoreQueryFilters().Count());
            repository.ClearQueryFilters();
            Assert.Equal(3L, repository.Count());
        }

        private async Task<List<CfNote>> SeedAsync(IRepository<CfNote> repository, int count)
        {
            await ResetAsync(typeof(CfNote));
            return (await repository.CreateManyAsync(Enumerable.Range(0, count).Select(i => new CfNote { AuthorId = 1, Text = "n" + i }).ToList(), null, Token)).ToList();
        }
    }
}
