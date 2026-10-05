namespace Test.Shared
{
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;

    /// <summary>
    /// A small library (companies, authors, books, categories and the author/category junction) seeded into a fresh
    /// <see cref="InMemoryBackend"/> for include and navigation tests.
    /// Authors: Austen (Penguin), Banks (Orbit), Christie (no company), Dick (dangling company id 999).
    /// Books: Emma and Persuasion by Austen; Excession and Player of Games by Banks.
    /// Categories: Classic (Austen, Banks, Christie), SciFi (Banks), Mystery (none).
    /// </summary>
    public sealed class InMemoryLibraryData
    {
        #region Public-Members

        /// <summary>Gets the backend. Never null.</summary>
        public InMemoryBackend Backend { get; }

        /// <summary>Gets the company repository. Never null.</summary>
        public InMemoryRepository<Company> Companies { get; }

        /// <summary>Gets the author repository. Never null.</summary>
        public InMemoryRepository<Author> Authors { get; }

        /// <summary>Gets the book repository. Never null.</summary>
        public InMemoryRepository<Book> Books { get; }

        /// <summary>Gets the category repository. Never null.</summary>
        public InMemoryRepository<Category> Categories { get; }

        /// <summary>Gets the junction repository. Never null.</summary>
        public InMemoryRepository<AuthorCategory> AuthorCategories { get; }

        /// <summary>Gets the Penguin company.</summary>
        public Company Penguin { get; private set; } = new Company();

        /// <summary>Gets the Orbit company.</summary>
        public Company Orbit { get; private set; } = new Company();

        /// <summary>Gets the author Austen.</summary>
        public Author Austen { get; private set; } = new Author();

        /// <summary>Gets the author Banks.</summary>
        public Author Banks { get; private set; } = new Author();

        /// <summary>Gets the author Christie.</summary>
        public Author Christie { get; private set; } = new Author();

        /// <summary>Gets the author Dick.</summary>
        public Author Dick { get; private set; } = new Author();

        /// <summary>Gets the Classic category.</summary>
        public Category Classic { get; private set; } = new Category();

        /// <summary>Gets the SciFi category.</summary>
        public Category SciFi { get; private set; } = new Category();

        /// <summary>Gets the Mystery category.</summary>
        public Category Mystery { get; private set; } = new Category();

        #endregion

        #region Constructors-and-Factories

        private InMemoryLibraryData(RepositoryCapabilities capabilities)
        {
            Backend = new InMemoryBackend(capabilities);
            Companies = Backend.CreateRepository<Company>();
            Authors = Backend.CreateRepository<Author>();
            Books = Backend.CreateRepository<Book>();
            Categories = Backend.CreateRepository<Category>();
            AuthorCategories = Backend.CreateRepository<AuthorCategory>();
        }

        /// <summary>
        /// Creates and seeds a library.
        /// </summary>
        /// <param name="capabilities">Backend capabilities. Default: all.</param>
        /// <returns>The seeded library.</returns>
        public static async Task<InMemoryLibraryData> CreateAsync(RepositoryCapabilities capabilities = RepositoryCapabilities.All)
        {
            InMemoryLibraryData data = new InMemoryLibraryData(capabilities);
            await data.SeedAsync().ConfigureAwait(false);
            return data;
        }

        #endregion

        #region Private-Methods

        private async Task SeedAsync()
        {
            Penguin = await Companies.CreateAsync(new Company { Name = "Penguin", Industry = "Publishing" }).ConfigureAwait(false);
            Orbit = await Companies.CreateAsync(new Company { Name = "Orbit", Industry = null }).ConfigureAwait(false);

            Austen = await Authors.CreateAsync(new Author { Name = "Austen", CompanyId = Penguin.Id }).ConfigureAwait(false);
            Banks = await Authors.CreateAsync(new Author { Name = "Banks", CompanyId = Orbit.Id }).ConfigureAwait(false);
            Christie = await Authors.CreateAsync(new Author { Name = "Christie", CompanyId = null }).ConfigureAwait(false);
            Dick = await Authors.CreateAsync(new Author { Name = "Dick", CompanyId = 999 }).ConfigureAwait(false);

            await Books.CreateAsync(new Book { Title = "Emma", AuthorId = Austen.Id, PublisherId = Penguin.Id }).ConfigureAwait(false);
            await Books.CreateAsync(new Book { Title = "Persuasion", AuthorId = Austen.Id, PublisherId = null }).ConfigureAwait(false);
            await Books.CreateAsync(new Book { Title = "Excession", AuthorId = Banks.Id, PublisherId = Orbit.Id }).ConfigureAwait(false);
            await Books.CreateAsync(new Book { Title = "Player of Games", AuthorId = Banks.Id, PublisherId = Orbit.Id }).ConfigureAwait(false);

            Classic = await Categories.CreateAsync(new Category { Name = "Classic" }).ConfigureAwait(false);
            SciFi = await Categories.CreateAsync(new Category { Name = "SciFi" }).ConfigureAwait(false);
            Mystery = await Categories.CreateAsync(new Category { Name = "Mystery" }).ConfigureAwait(false);

            await AuthorCategories.CreateAsync(new AuthorCategory { AuthorId = Austen.Id, CategoryId = Classic.Id }).ConfigureAwait(false);
            await AuthorCategories.CreateAsync(new AuthorCategory { AuthorId = Banks.Id, CategoryId = SciFi.Id }).ConfigureAwait(false);
            await AuthorCategories.CreateAsync(new AuthorCategory { AuthorId = Banks.Id, CategoryId = Classic.Id }).ConfigureAwait(false);
            await AuthorCategories.CreateAsync(new AuthorCategory { AuthorId = Christie.Id, CategoryId = Classic.Id }).ConfigureAwait(false);
        }

        #endregion
    }
}
