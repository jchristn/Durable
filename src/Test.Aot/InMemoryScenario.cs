namespace Test.Aot
{
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;

    /// <summary>
    /// In-memory backend end-to-end checks (the shared repository scenario over <see cref="InMemoryBackend"/>).
    /// </summary>
    internal static class InMemoryScenario
    {
        public static async Task RunAsync(CheckRunner runner)
        {
            using InMemoryBackend backend = InMemoryBackend.Create(new InMemoryRepositorySettings { JsonOptions = DurableJson.CreateOptions(AotJsonContext.Default) });
            using InMemoryRepository<Author> authors = new InMemoryRepository<Author>(backend);
            using InMemoryRepository<Book> books = new InMemoryRepository<Book>(backend);
            using InMemoryRepository<Review> reviews = new InMemoryRepository<Review>(backend);
            using InMemoryRepository<Category> categories = new InMemoryRepository<Category>(backend);
            using InMemoryRepository<AuthorCategory> links = new InMemoryRepository<AuthorCategory>(backend);
            using InMemoryRepository<Shelf> shelves = new InMemoryRepository<Shelf>(backend);

            await RepositoryScenario.RunAsync(runner, new RepositorySet("inmemory", authors, books, reviews, categories, links, shelves)).ConfigureAwait(false);
        }
    }
}
