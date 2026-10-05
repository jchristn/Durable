namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// The shared relationship graph:
    /// publishers Penguin, Vintage, Empty Press (no authors);
    /// authors Austen (Penguin; books Emma/Penguin, Persuasion/Vintage, Sanditon/none; tags Classic, Satire; notes keep-a, drop-a deleted),
    /// Borges (Vintage; books Ficciones/Vintage, Labyrinths/Penguin; tag Fantasy; note drop-b deleted),
    /// Calvino (Penguin; no books, tags or notes),
    /// Dickens (no publisher; book Hard Times/none; tags Classic, Satire; note keep-d);
    /// tag Unused has no authors.
    /// </summary>
    internal sealed class LibraryFixture
    {
        private LibraryFixture()
        {
        }

        internal Dictionary<string, CfPublisher> Publishers { get; } = new Dictionary<string, CfPublisher>(StringComparer.Ordinal);

        internal Dictionary<string, CfAuthor> Authors { get; } = new Dictionary<string, CfAuthor>(StringComparer.Ordinal);

        internal Dictionary<string, CfBook> Books { get; } = new Dictionary<string, CfBook>(StringComparer.Ordinal);

        internal Dictionary<string, CfTag> Tags { get; } = new Dictionary<string, CfTag>(StringComparer.Ordinal);

        internal static async Task<LibraryFixture> CreateAsync(
            IRepository<CfPublisher> publishers,
            IRepository<CfAuthor> authors,
            IRepository<CfBook> books,
            IRepository<CfTag> tags,
            IRepository<CfAuthorTag> links,
            IRepository<CfNote> notes,
            CancellationToken token)
        {
            LibraryFixture fixture = new LibraryFixture();

            foreach (string name in new[] { "Penguin", "Vintage", "Empty Press" })
                fixture.Publishers[name] = await publishers.CreateAsync(new CfPublisher { Name = name }, null, token).ConfigureAwait(false);

            fixture.Authors["Austen"] = await authors.CreateAsync(new CfAuthor { Name = "Austen", PublisherId = fixture.Publishers["Penguin"].Id }, null, token).ConfigureAwait(false);
            fixture.Authors["Borges"] = await authors.CreateAsync(new CfAuthor { Name = "Borges", PublisherId = fixture.Publishers["Vintage"].Id }, null, token).ConfigureAwait(false);
            fixture.Authors["Calvino"] = await authors.CreateAsync(new CfAuthor { Name = "Calvino", PublisherId = fixture.Publishers["Penguin"].Id }, null, token).ConfigureAwait(false);
            fixture.Authors["Dickens"] = await authors.CreateAsync(new CfAuthor { Name = "Dickens", PublisherId = null }, null, token).ConfigureAwait(false);

            await AddBookAsync(fixture, books, "Emma", 474, "Austen", "Penguin", token).ConfigureAwait(false);
            await AddBookAsync(fixture, books, "Persuasion", 249, "Austen", "Vintage", token).ConfigureAwait(false);
            await AddBookAsync(fixture, books, "Sanditon", 120, "Austen", null, token).ConfigureAwait(false);
            await AddBookAsync(fixture, books, "Ficciones", 174, "Borges", "Vintage", token).ConfigureAwait(false);
            await AddBookAsync(fixture, books, "Labyrinths", 251, "Borges", "Penguin", token).ConfigureAwait(false);
            await AddBookAsync(fixture, books, "Hard Times", 352, "Dickens", null, token).ConfigureAwait(false);

            foreach (string label in new[] { "Classic", "Fantasy", "Satire", "Unused" })
                fixture.Tags[label] = await tags.CreateAsync(new CfTag { Label = label }, null, token).ConfigureAwait(false);

            await links.CreateManyAsync(new List<CfAuthorTag>
            {
                new CfAuthorTag { AuthorId = fixture.Authors["Austen"].Id, TagId = fixture.Tags["Classic"].Id },
                new CfAuthorTag { AuthorId = fixture.Authors["Austen"].Id, TagId = fixture.Tags["Satire"].Id },
                new CfAuthorTag { AuthorId = fixture.Authors["Borges"].Id, TagId = fixture.Tags["Fantasy"].Id },
                new CfAuthorTag { AuthorId = fixture.Authors["Dickens"].Id, TagId = fixture.Tags["Classic"].Id },
                new CfAuthorTag { AuthorId = fixture.Authors["Dickens"].Id, TagId = fixture.Tags["Satire"].Id }
            }, null, token).ConfigureAwait(false);

            await notes.CreateAsync(new CfNote { AuthorId = fixture.Authors["Austen"].Id, Text = "keep-a" }, null, token).ConfigureAwait(false);
            CfNote dropA = await notes.CreateAsync(new CfNote { AuthorId = fixture.Authors["Austen"].Id, Text = "drop-a" }, null, token).ConfigureAwait(false);
            CfNote dropB = await notes.CreateAsync(new CfNote { AuthorId = fixture.Authors["Borges"].Id, Text = "drop-b" }, null, token).ConfigureAwait(false);
            await notes.CreateAsync(new CfNote { AuthorId = fixture.Authors["Dickens"].Id, Text = "keep-d" }, null, token).ConfigureAwait(false);
            await notes.DeleteAsync(dropA, null, token).ConfigureAwait(false);
            await notes.DeleteAsync(dropB, null, token).ConfigureAwait(false);

            return fixture;
        }

        internal static string[] Titles(IEnumerable<CfBook> books)
        {
            return books.Select(b => b.Title).OrderBy(t => t, StringComparer.Ordinal).ToArray();
        }

        private static async Task AddBookAsync(LibraryFixture fixture, IRepository<CfBook> books, string title, int pages, string author, string? publisher, CancellationToken token)
        {
            CfBook book = new CfBook
            {
                Title = title,
                Pages = pages,
                AuthorId = fixture.Authors[author].Id,
                PublisherId = publisher == null ? null : fixture.Publishers[publisher].Id
            };
            fixture.Books[title] = await books.CreateAsync(book, null, token).ConfigureAwait(false);
        }
    }
}
