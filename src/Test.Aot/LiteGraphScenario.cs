namespace Test.Aot
{
    using System.Threading.Tasks;
    using Durable;
    using Durable.LiteGraph;

    /// <summary>
    /// LiteGraph backend end-to-end checks (the shared repository scenario over an ephemeral LiteGraph database).
    /// </summary>
    internal static class LiteGraphScenario
    {
        public static async Task RunAsync(CheckRunner runner)
        {
            await using LiteGraphBackend backend = await LiteGraphBackend.CreateAsync(new LiteGraphRepositorySettings
            {
                JsonOptions = DurableJson.CreateOptions(AotJsonContext.Default)
            }).ConfigureAwait(false);
            using LiteGraphRepository<Author> authors = backend.CreateRepository<Author>();
            using LiteGraphRepository<Book> books = backend.CreateRepository<Book>();
            using LiteGraphRepository<Review> reviews = backend.CreateRepository<Review>();
            using LiteGraphRepository<Category> categories = backend.CreateRepository<Category>();
            using LiteGraphRepository<AuthorCategory> links = backend.CreateRepository<AuthorCategory>();
            using LiteGraphRepository<Shelf> shelves = backend.CreateRepository<Shelf>();

            await RepositoryScenario.RunAsync(runner, new RepositorySet("litegraph", authors, books, reviews, categories, links, shelves)).ConfigureAwait(false);
        }
    }
}
