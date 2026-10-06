namespace Test.Aot
{
    using System.Threading.Tasks;
    using Durable;
    using Durable.LiteDb;

    /// <summary>
    /// LiteDB backend end-to-end checks (the shared repository scenario over an in-memory LiteDB database).
    /// </summary>
    internal static class LiteDbScenario
    {
        public static async Task RunAsync(CheckRunner runner)
        {
            using LiteDbBackend backend = new LiteDbBackend(
                new LiteDbRepositorySettings(LiteDbRepositorySettings.InMemoryFilename),
                DurableJson.CreateOptions(AotJsonContext.Default));
            using LiteDbRepository<Author> authors = new LiteDbRepository<Author>(backend);
            using LiteDbRepository<Book> books = new LiteDbRepository<Book>(backend);
            using LiteDbRepository<Review> reviews = new LiteDbRepository<Review>(backend);
            using LiteDbRepository<Category> categories = new LiteDbRepository<Category>(backend);
            using LiteDbRepository<AuthorCategory> links = new LiteDbRepository<AuthorCategory>(backend);
            using LiteDbRepository<Shelf> shelves = new LiteDbRepository<Shelf>(backend);

            await RepositoryScenario.RunAsync(runner, new RepositorySet("litedb", authors, books, reviews, categories, links, shelves)).ConfigureAwait(false);
        }
    }
}
