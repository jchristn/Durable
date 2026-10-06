namespace Test.Aot
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Backend-neutral end-to-end checks run against every backend in the AOT app.
    /// </summary>
    internal static class RepositoryScenario
    {
        public static async Task RunAsync(CheckRunner runner, RepositorySet set)
        {
            string p = "[" + set.Label + "] ";
            DateTime baseDate = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            // ---------------------------------------------------------------- CRUD (sync + async)
            Author ada = set.Authors.Create(new Author { Name = "Ada Lovelace", Email = "ada@example.com", Rating = 5, Status = AuthorStatus.Active, Born = baseDate.AddYears(-200), Score = 9.5m });
            runner.Check(p + "Create (sync) assigns identity", () => ada.Id > 0);

            Author alan = await set.Authors.CreateAsync(new Author { Name = "Alan Turing", Email = null, Rating = 4, Status = AuthorStatus.Retired, Born = baseDate.AddYears(-100) }).ConfigureAwait(false);
            runner.Check(p + "Create (async) assigns identity", () => alan.Id > 0 && alan.Id != ada.Id);

            List<Author> many = set.Authors.CreateMany(new[]
            {
                new Author { Name = "Grace Hopper", Email = "grace@example.com", Rating = 5, Status = AuthorStatus.Active, Born = baseDate.AddYears(-110), Score = 8m },
                new Author { Name = "Edsger Dijkstra", Email = "ewd@example.com", Rating = 3, Status = AuthorStatus.Retired, Born = baseDate.AddYears(-90) },
                new Author { Name = "Barbara Liskov", Email = "barbara@example.com", Rating = 4, Status = AuthorStatus.Active, Born = baseDate.AddYears(-80), Score = 7.25m }
            }).ToList();
            runner.Check(p + "CreateMany returns keys", () => many.Count == 3 && many.All(a => a.Id > 0));

            runner.Check(p + "ReadById (sync)", () => set.Authors.ReadById(ada.Id)?.Name == "Ada Lovelace");
            await runner.CheckAsync(p + "ReadByIdAsync", async () => (await set.Authors.ReadByIdAsync(alan.Id).ConfigureAwait(false))?.Status == AuthorStatus.Retired).ConfigureAwait(false);

            ada.Rating = 6;
            set.Authors.Update(ada);
            runner.Check(p + "Update (sync)", () => set.Authors.ReadById(ada.Id)!.Rating == 6);
            alan.Email = "alan@example.com";
            await set.Authors.UpdateAsync(alan).ConfigureAwait(false);
            await runner.CheckAsync(p + "UpdateAsync", async () => (await set.Authors.ReadByIdAsync(alan.Id).ConfigureAwait(false))!.Email == "alan@example.com").ConfigureAwait(false);

            // ---------------------------------------------------------------- LINQ predicates
            string prefix = "Gr";
            AuthorStatus retired = AuthorStatus.Retired;
            DateTime cutoff = baseDate.AddYears(-105);
            int[] ids = new[] { ada.Id, alan.Id };
            List<string> names = new List<string> { "Grace Hopper", "Barbara Liskov" };

            runner.Check(p + "Where string equality", () => set.Authors.ReadMany(a => a.Name == "Alan Turing").Single().Id == alan.Id);
            runner.Check(p + "Where StartsWith", () => set.Authors.ReadMany(a => a.Name.StartsWith(prefix)).Single().Name == "Grace Hopper");
            runner.Check(p + "Where Contains (string)", () => set.Authors.ReadMany(a => a.Name.Contains("Lis")).Single().Name == "Barbara Liskov");
            runner.Check(p + "Where EndsWith", () => set.Authors.ReadMany(a => a.Name.EndsWith("stra")).Count() == 1);
            runner.Check(p + "Where enum", () => set.Authors.ReadMany(a => a.Status == retired).Count() == 2);
            runner.Check(p + "Where date comparison", () => set.Authors.ReadMany(a => a.Born > cutoff).Count() == 3);
            runner.Check(p + "Where nullable == null", () => set.Authors.ReadMany(a => a.Score == null).Count() == 2);
            runner.Check(p + "Where nullable HasValue && Value", () => set.Authors.ReadMany(a => a.Score.HasValue && a.Score.Value > 7.5m).Count() == 2);
            runner.Check(p + "Where array Contains", () => set.Authors.ReadMany(a => ids.Contains(a.Id)).Count() == 2);
            runner.Check(p + "Where List Contains", () => set.Authors.ReadMany(a => names.Contains(a.Name)).Count() == 2);
            runner.Check(p + "Where ToUpper/Length", () => set.Authors.ReadMany(a => a.Name.ToUpper() == "ADA LOVELACE" && a.Name.Length == 12).Count() == 1);

            // ---------------------------------------------------------------- ordering, paging, aggregates
            runner.Check(p + "OrderBy/ThenBy + Skip/Take", () =>
            {
                List<Author> page = set.Authors.Query().OrderByDescending(a => a.Rating).ThenBy(a => a.Name).Skip(1).Take(2).Execute().ToList();
                return page.Count == 2 && page[0].Name == "Grace Hopper" && page[1].Name == "Alan Turing";
            });
            runner.Check(p + "Count", () => set.Authors.Count() == 5 && set.Authors.Query().Where(a => a.Rating >= 4).Count() == 4);
            runner.Check(p + "Sum", () => set.Authors.Sum(a => a.Rating) == 22m);
            runner.Check(p + "Average", () => set.Authors.Query().Average(a => a.Rating) == 4.4m);
            runner.Check(p + "Min/Max", () => set.Authors.Min(a => a.Born) == baseDate.AddYears(-200) && set.Authors.Query().Max(a => a.Rating) == 6);
            await runner.CheckAsync(p + "CountAsync/SumAsync", async () =>
                await set.Authors.CountAsync(a => a.Status == AuthorStatus.Active).ConfigureAwait(false) == 3
                && await set.Authors.Query().SumAsync(a => a.Score).ConfigureAwait(false) == 24.75m).ConfigureAwait(false);
            runner.Check(p + "Any/Exists", () => set.Authors.Exists(a => a.Name == "Ada Lovelace") && !set.Authors.Query().Where(a => a.Rating > 100).Any());

            // ---------------------------------------------------------------- projection and grouping
            RunProjections(runner, set, p);

            // ---------------------------------------------------------------- relationships, JSON, value converter
            Category science = set.Categories.Create(new Category { Name = "Science" });
            Category math = set.Categories.Create(new Category { Name = "Math" });
            set.Links.Create(new AuthorCategory { AuthorId = ada.Id, CategoryId = science.Id });
            set.Links.Create(new AuthorCategory { AuthorId = ada.Id, CategoryId = math.Id });
            set.Links.Create(new AuthorCategory { AuthorId = alan.Id, CategoryId = math.Id });

            Book engine = set.Books.Create(new Book
            {
                Title = "Notes on the Analytical Engine",
                AuthorId = ada.Id,
                Price = 12.50m,
                Published = baseDate.AddYears(-180),
                Isbn = new Isbn("978-0-00-000001-1"),
                Tags = new List<string> { "computing", "history" },
                Details = new BookDetails { Pages = 64, Publisher = "Taylor", Extra = new Dictionary<string, string> { { "lang", "en" } } }
            });
            Book computable = set.Books.Create(new Book { Title = "On Computable Numbers", AuthorId = alan.Id, Price = 30m, Published = null, Tags = new List<string> { "logic" } });
            set.Reviews.Create(new Review { BookId = engine.Id, Stars = 5, Text = "Visionary" });
            set.Reviews.Create(new Review { BookId = engine.Id, Stars = 4 });
            set.Reviews.Create(new Review { BookId = computable.Id, Stars = 5 });

            runner.Check(p + "JSON columns round-trip (source-generated context)", () =>
            {
                Book read = set.Books.ReadById(engine.Id)!;
                return read.Tags.SequenceEqual(new[] { "computing", "history" })
                    && read.Details != null && read.Details.Pages == 64 && read.Details.Publisher == "Taylor" && read.Details.Extra["lang"] == "en";
            });
            runner.Check(p + "Value converter round-trip", () => set.Books.ReadById(engine.Id)!.Isbn?.Value == "978-0-00-000001-1" && set.Books.ReadById(computable.Id)!.Isbn == null);
            runner.Check(p + "Where nullable DateTime", () => set.Books.ReadMany(b => b.Published == null).Single().Id == computable.Id);

            runner.Check(p + "Include one-to-many + ThenInclude", () =>
            {
                Author loaded = set.Authors.Query().Where(a => a.Id == ada.Id).Include(a => a.Books).ThenInclude<Book, List<Review>>(b => b.Reviews).Execute().Single();
                return loaded.Books.Count == 1 && loaded.Books[0].Title == "Notes on the Analytical Engine" && loaded.Books[0].Reviews.Count == 2;
            });
            runner.Check(p + "Include many-to-many", () =>
            {
                List<Author> loaded = set.Authors.Query().Where(a => a.Id == ada.Id || a.Id == alan.Id).OrderBy(a => a.Id).Include(a => a.Categories).Execute().ToList();
                return loaded[0].Categories.Select(c => c.Name).OrderBy(n => n).SequenceEqual(new[] { "Math", "Science" }) && loaded[1].Categories.Single().Name == "Math";
            });
            runner.Check(p + "Include reference navigation", () =>
            {
                List<Book> loaded = set.Books.Query().OrderBy(b => b.Id).Include(b => b.Author).Execute().ToList();
                return loaded.Count == 2 && loaded[0].Author?.Name == "Ada Lovelace" && loaded[1].Author?.Name == "Alan Turing";
            });
            runner.Check(p + "Navigation predicates (reference + collection Any)", () =>
                set.Books.ReadMany(b => b.Author!.Name == "Alan Turing").Single().Id == computable.Id
                && set.Authors.ReadMany(a => a.Books.Any(b => b.Price > 20m)).Single().Id == alan.Id);

            // ---------------------------------------------------------------- transactions
            runner.Check(p + "Transaction rollback (sync)", () =>
            {
                using (ITransaction tx = set.Categories.BeginTransaction())
                {
                    set.Categories.Create(new Category { Name = "Rolled back" }, tx);
                    tx.Rollback();
                }

                return !set.Categories.Exists(c => c.Name == "Rolled back");
            });
            await runner.CheckAsync(p + "Transaction commit (async)", async () =>
            {
                ITransaction tx = await set.Categories.BeginTransactionAsync().ConfigureAwait(false);
                await using (tx.ConfigureAwait(false))
                {
                    await set.Categories.CreateAsync(new Category { Name = "Committed" }, tx).ConfigureAwait(false);
                    await tx.CommitAsync().ConfigureAwait(false);
                }

                return await set.Categories.ExistsAsync(c => c.Name == "Committed").ConfigureAwait(false);
            }).ConfigureAwait(false);

            // ---------------------------------------------------------------- optimistic concurrency
            runner.Check(p + "Optimistic concurrency conflict detected", () =>
            {
                Book first = set.Books.ReadById(engine.Id)!;
                Book second = set.Books.ReadById(engine.Id)!;
                first.Price = 13m;
                set.Books.Update(first);
                second.Price = 14m;
                try
                {
                    set.Books.Update(second);
                    return false;
                }
                catch (OptimisticConcurrencyException)
                {
                    return set.Books.ReadById(engine.Id)!.Price == 13m;
                }
            });

            // ---------------------------------------------------------------- soft delete and delete
            runner.Check(p + "Soft delete hides rows, IgnoreQueryFilters shows them", () =>
            {
                Author dijkstra = set.Authors.ReadFirst(a => a.Name == "Edsger Dijkstra")!;
                bool deleted = set.Authors.Delete(dijkstra);
                return deleted && set.Authors.Count() == 4 && set.Authors.Query().IgnoreQueryFilters().Count() == 5;
            });
            await runner.CheckAsync(p + "DeleteByIdAsync", async () =>
            {
                Category temp = await set.Categories.CreateAsync(new Category { Name = "Temp" }).ConfigureAwait(false);
                bool deleted = await set.Categories.DeleteByIdAsync(temp.Id).ConfigureAwait(false);
                return deleted && !set.Categories.ExistsById(temp.Id);
            }).ConfigureAwait(false);
            await runner.CheckAsync(p + "ReadManyAsync streams", async () =>
            {
                int count = 0;
                await foreach (Author a in set.Authors.ReadManyAsync(x => x.Rating >= 4).ConfigureAwait(false)) count++;
                return count == 4;
            }).ConfigureAwait(false);

            // ---------------------------------------------------------------- trimming diagnostic for an unrooted navigation target
            Shelf shelf = set.Shelves.Create(new Shelf { Name = "Top" });
            runner.Check(p + "Unrooted navigation target: works on JIT, explicit diagnostic under Native AOT", () =>
            {
                try
                {
                    Shelf loaded = set.Shelves.Query().Where(s => s.Id == shelf.Id).Include(s => s.Items).Execute().Single();
                    return !RuntimeMode.IsNativeAot && loaded.Items.Count == 0;
                }
                catch (InvalidOperationException ex) when (RuntimeMode.IsNativeAot)
                {
                    return ex.Message.Contains("EntityMetadata.For<ShelfItem>()", StringComparison.Ordinal);
                }
            });
        }

        // The C# compiler lowers object initializers in expression trees to Expression.Bind(MethodInfo, ...), which is
        // annotated RequiresUnreferencedCode because it looks up the property of the setter. Durable annotates
        // Select<TResult> so TResult's public properties are kept, which is exactly what Expression.Bind needs.
        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Member-init projections: Select<TResult> keeps TResult's public properties (DynamicallyAccessedMembers), so Expression.Bind finds them.")]
        private static void RunProjections(CheckRunner runner, RepositorySet set, string p)
        {
            runner.Check(p + "Select projection to DTO", () =>
            {
                List<AuthorSummary> rows = set.Authors.Query().Where(a => a.Rating >= 5).OrderBy(a => a.Name)
                    .Select(a => new AuthorSummary { Name = a.Name, Rating = a.Rating }).Execute().ToList();
                return rows.Count == 2 && rows[0].Name == "Ada Lovelace" && rows[0].Rating == 6 && rows[1].Name == "Grace Hopper";
            });
            runner.Check(p + "GroupBy + Select aggregate DTO", () =>
            {
                List<StatusCount> groups = set.Authors.Query().GroupBy(a => a.Status)
                    .Select(g => new StatusCount { Status = g.Key, Count = g.Count(), TotalRating = g.Sum(x => x.Rating) })
                    .Execute().OrderBy(g => g.Status).ToList();
                return groups.Count == 2
                    && groups[0].Status == AuthorStatus.Active && groups[0].Count == 3 && groups[0].TotalRating == 15
                    && groups[1].Status == AuthorStatus.Retired && groups[1].Count == 2 && groups[1].TotalRating == 7;
            });
            runner.Check(p + "GroupBy Execute (groupings)", () =>
            {
                List<IGrouping<AuthorStatus, Author>> groups = set.Authors.Query().GroupBy(a => a.Status).Execute().ToList();
                return groups.Count == 2 && groups.Sum(g => g.Count()) == 5;
            });
        }
    }
}
