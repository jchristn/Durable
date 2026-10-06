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
            await using LiteDbBackend backend = await LiteDbBackend.CreateAsync(new LiteDbRepositorySettings
            {
                JsonOptions = DurableJson.CreateOptions(AotJsonContext.Default)
            }).ConfigureAwait(false);
            using LiteDbRepository<Author> authors = backend.CreateRepository<Author>();
            using LiteDbRepository<Book> books = backend.CreateRepository<Book>();
            using LiteDbRepository<Review> reviews = backend.CreateRepository<Review>();
            using LiteDbRepository<Category> categories = backend.CreateRepository<Category>();
            using LiteDbRepository<AuthorCategory> links = backend.CreateRepository<AuthorCategory>();
            using LiteDbRepository<Shelf> shelves = backend.CreateRepository<Shelf>();

            await RepositoryScenario.RunAsync(runner, new RepositorySet("litedb", authors, books, reviews, categories, links, shelves)).ConfigureAwait(false);
        }
    }
}
