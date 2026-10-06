namespace Test.Aot
{
    using Durable;

    /// <summary>
    /// The repositories one backend provides to <see cref="RepositoryScenario"/>.
    /// </summary>
    internal sealed class RepositorySet
    {
        public RepositorySet(
            string label,
            IRepository<Author> authors,
            IRepository<Book> books,
            IRepository<Review> reviews,
            IRepository<Category> categories,
            IRepository<AuthorCategory> links,
            IRepository<Shelf> shelves)
        {
            Label = label;
            Authors = authors;
            Books = books;
            Reviews = reviews;
            Categories = categories;
            Links = links;
            Shelves = shelves;
        }

        public string Label { get; }

        public IRepository<Author> Authors { get; }

        public IRepository<Book> Books { get; }

        public IRepository<Review> Reviews { get; }

        public IRepository<Category> Categories { get; }

        public IRepository<AuthorCategory> Links { get; }

        public IRepository<Shelf> Shelves { get; }
    }
}
