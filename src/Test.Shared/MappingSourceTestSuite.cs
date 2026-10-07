namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;
    using Durable.LiteDb;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Entity mapping sources (<see cref="IEntityMappingSource"/>): classes that carry no Durable attributes are mapped by
    /// <see cref="MapAttributeMappingSource"/>, which translates test-only Map* attributes. Covers the metadata the source
    /// produces, CRUD, composite and auto-increment keys, reference, collection and many-to-many includes, a version
    /// conflict, soft delete, a value converter, a generated default, indexes created by InitializeTable, an empty schema
    /// diff, the in-memory and LiteDB backends, the global <see cref="DurableMapping.MappingSource"/> and the rule that a
    /// built type's mapping cannot change.
    /// Every test uses the Ms* entities, which no other suite touches, so registration cannot leak into other suites.
    /// </summary>
    public class MappingSourceTestSuite
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="MappingSourceTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public MappingSourceTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            MsMappingRegistration.EnsureRegistered();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// The metadata built from the source has the translated table, columns, keys, navigations, version and soft-delete
        /// columns, converter, default value and indexes; a class with a table but no columns is convention mapped.
        /// </summary>
        [Fact]
        public void MetadataComesFromTheRegisteredSource()
        {
            EntityMetadata author = EntityMetadata.For<MsAuthor>();
            Assert.Same(MsMappingRegistration.Source, author.MappingSource);
            Assert.True(author.HasEntityAttribute);
            Assert.False(author.IsConventionMapped);
            Assert.Equal("ms_authors", author.TableName);
            Assert.Equal(new[] { "author_id", "author_name" }, author.Columns.Select(c => c.Name).ToArray());
            Assert.Same(author.AutoIncrementColumn, Assert.Single(author.KeyColumns));
            Assert.Equal(64, author.FindColumnByProperty("Name")!.MaxLength);
            Assert.Null(author.FindColumnByProperty("Scratch"));
            Assert.Equal("idx_ms_authors_name", Assert.Single(author.FindColumnByProperty("Name")!.Indexes).Name);
            Assert.Equal(NavigationKind.Collection, author.FindNavigation("Books")!.Kind);
            Assert.Equal(NavigationKind.ManyToMany, author.FindNavigation("Tags")!.Kind);

            EntityMetadata book = EntityMetadata.For<MsBook>();
            Assert.Equal(NavigationKind.Reference, book.FindNavigation("Author")!.Kind);
            Assert.Equal(typeof(MsAuthor), book.FindColumnByProperty("AuthorId")!.ForeignKey!.ReferencedType);
            Assert.NotNull(book.FindColumnByProperty("Price")!.Converter);

            EntityMetadata junction = EntityMetadata.For<MsAuthorTag>();
            Assert.Equal(new[] { "author_ref", "tag_ref" }, junction.KeyColumns.Select(c => c.Name).ToArray());
            Assert.Null(junction.AutoIncrementColumn);

            EntityMetadata document = EntityMetadata.For<MsDocument>();
            Assert.Equal("revision", document.VersionColumn!.Name);
            Assert.Equal(VersionColumnType.Integer, document.VersionInfo!.Type);
            Assert.Equal("is_deleted", document.SoftDeleteColumn!.Name);
            Assert.NotNull(document.FindColumnByProperty("PublicId")!.DefaultValue);
            CompositeIndexAttribute composite = Assert.Single(document.CompositeIndexes);
            Assert.Equal(new[] { "owner", "title" }, composite.ColumnNames);
            Assert.True(composite.IsUnique);

            EntityMetadata convention = EntityMetadata.For<MsConventionItem>();
            Assert.True(convention.IsConventionMapped);
            Assert.Equal("ms_convention_items", convention.TableName);
            Assert.Equal(new[] { "Id", "Name" }, convention.Columns.Select(c => c.Name).ToArray());
            Assert.True(Assert.Single(convention.KeyColumns).IsAutoIncrement);

            EntityMetadata overridden = EntityMetadata.For<MsOverrideItem>();
            Assert.Equal("ms_override_items", overridden.TableName);
            Assert.Equal(new[] { "item_id", "item_name" }, overridden.Columns.Select(c => c.Name).ToArray());

            Assert.Same(DurableMapping.AttributeSource, EntityMetadata.For<Author>().MappingSource);
            Assert.Equal("authors", DurableMapping.AttributeSource.GetEntityAttribute(typeof(Author))!.Name);
            Assert.Same(MsMappingRegistration.Source, DurableMapping.GetMappingSource(typeof(MsBook)));
        }

        /// <summary>
        /// Create, read, update and delete work through the provider for a source-mapped class, sync and async, including a
        /// value-converted column, and the SQL uses the translated names.
        /// </summary>
        [Fact]
        public async Task CrudUsesTheTranslatedMapping()
        {
            ISqlRepository<MsAuthor> authors = _Provider.CreateRepository<MsAuthor>();
            ISqlRepository<MsBook> books = _Provider.CreateRepository<MsBook>();
            await RelTestHelpers.RecreateTableAsync(authors);
            await RelTestHelpers.RecreateTableAsync(books);

            MsAuthor austen = await authors.CreateAsync(new MsAuthor { Name = "Austen", Scratch = "not stored" });
            MsAuthor banks = authors.Create(new MsAuthor { Name = "Banks" });
            Assert.True(austen.Id > 0);
            Assert.NotEqual(austen.Id, banks.Id);

            authors.CaptureSql = true;
            MsAuthor? read = await authors.ReadByIdAsync(austen.Id);
            Assert.Equal("Austen", read!.Name);
            Assert.Equal(string.Empty, read.Scratch);
            Assert.Contains("author_id", authors.LastExecutedSql ?? string.Empty, StringComparison.Ordinal);

            read.Name = "Jane Austen";
            await authors.UpdateAsync(read);
            Assert.Equal("Jane Austen", authors.ReadById(austen.Id)!.Name);
            Assert.Equal(new[] { "Banks" }, (await authors.Query().Where(a => a.Name.StartsWith("B")).ExecuteAsync()).Select(a => a.Name).ToArray());

            MsBook emma = await books.CreateAsync(new MsBook { Title = "Emma", AuthorId = austen.Id, Price = new Money(1299) });
            Assert.Equal(new Money(1299), (await books.ReadByIdAsync(emma.Id))!.Price);
            Assert.Equal(1, await books.CountAsync(b => b.Price == new Money(1299)));

            Assert.True(await authors.DeleteByIdAsync(banks.Id));
            Assert.Equal(1, await authors.CountAsync());
        }

        /// <summary>
        /// Reference, collection and many-to-many includes load through the translated navigations; the junction's
        /// composite key works with by-key reads.
        /// </summary>
        [Fact]
        public async Task IncludesAndCompositeKeysWork()
        {
            ISqlRepository<MsAuthor> authors = _Provider.CreateRepository<MsAuthor>();
            ISqlRepository<MsBook> books = _Provider.CreateRepository<MsBook>();
            ISqlRepository<MsTag> tags = _Provider.CreateRepository<MsTag>();
            ISqlRepository<MsAuthorTag> links = _Provider.CreateRepository<MsAuthorTag>();
            await RelTestHelpers.RecreateTableAsync(authors);
            await RelTestHelpers.RecreateTableAsync(books);
            await RelTestHelpers.RecreateTableAsync(tags);
            await RelTestHelpers.RecreateTableAsync(links);
            await SeedLibraryAsync(authors, books, tags, links);

            await AssertLibraryAsync(authors, books, links);
        }

        /// <summary>
        /// A stale update of a source-mapped version column throws; soft delete hides the row from reads and counts but
        /// leaves it in the table; the generated default fills the public id.
        /// </summary>
        [Fact]
        public async Task VersionColumnSoftDeleteAndDefaultsWork()
        {
            ISqlRepository<MsDocument> documents = _Provider.CreateRepository<MsDocument>();
            await RelTestHelpers.RecreateTableAsync(documents);
            await AssertDocumentRulesAsync(documents);
        }

        /// <summary>
        /// InitializeTable creates the translated columns, key and indexes (single and unique composite), and a schema diff
        /// against the source-mapped classes is empty.
        /// </summary>
        [Fact]
        public async Task SchemaMatchesTheTranslatedMapping()
        {
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            ISqlRepository<MsAuthor> authors = _Provider.CreateRepository<MsAuthor>();
            ISqlRepository<MsDocument> documents = _Provider.CreateRepository<MsDocument>();
            ISqlRepository<MsAuthorTag> links = _Provider.CreateRepository<MsAuthorTag>();
            await RelTestHelpers.RecreateTableAsync(authors);
            await RelTestHelpers.RecreateTableAsync(documents);
            await RelTestHelpers.RecreateTableAsync(links);

            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
            TableSchema authorTable = (await reader.ReadTableAsync("ms_authors"))!;
            Assert.Equal(new[] { "author_id" }, authorTable.PrimaryKeyColumns.ToArray(), StringComparer.OrdinalIgnoreCase);
            Assert.NotNull(authorTable.FindColumn("author_name"));
            Assert.Null(authorTable.FindColumn("Scratch"));
            Assert.NotNull(authorTable.FindIndex("idx_ms_authors_name"));

            TableSchema documentTable = (await reader.ReadTableAsync("ms_documents"))!;
            IndexSchema composite = documentTable.FindIndex("idx_ms_documents_owner_title")!;
            Assert.True(composite.IsUnique);
            Assert.Equal(new[] { "owner", "title" }, composite.Columns.ToArray(), StringComparer.OrdinalIgnoreCase);

            TableSchema linkTable = (await reader.ReadTableAsync("ms_author_tags"))!;
            Assert.Equal(new[] { "author_ref", "tag_ref" }, linkTable.PrimaryKeyColumns.ToArray(), StringComparer.OrdinalIgnoreCase);

            SqlMigrator migrator = new SqlMigrator(factory, _Provider.Dialect);
            SchemaDiff diff = await migrator.DiffSchemaAsync(new[] { EntityMetadata.For<MsAuthor>(), EntityMetadata.For<MsDocument>(), EntityMetadata.For<MsAuthorTag>() });
            Assert.True(diff.IsEmpty, "Expected no differences but got: " + string.Join(" | ", diff.Operations.Select(o => o.ToString()).Concat(diff.Differences.Select(d => d.ToString()))));
        }

        /// <summary>
        /// The same source-mapped classes behave identically on the in-memory and LiteDB backends: includes, composite keys,
        /// the version conflict, soft delete and defaults.
        /// </summary>
        [Fact]
        public async Task NonSqlBackendsUseTheSameMapping()
        {
            using (InMemoryBackend memory = InMemoryBackend.Create())
            {
                InMemoryRepository<MsAuthor> authors = memory.CreateRepository<MsAuthor>();
                InMemoryRepository<MsBook> books = memory.CreateRepository<MsBook>();
                InMemoryRepository<MsAuthorTag> links = memory.CreateRepository<MsAuthorTag>();
                await SeedLibraryAsync(authors, books, memory.CreateRepository<MsTag>(), links);
                await AssertLibraryAsync(authors, books, links);
                await AssertDocumentRulesAsync(memory.CreateRepository<MsDocument>());
            }

            using (LiteDbBackend lite = LiteDbBackend.Create(LiteDbRepositorySettings.ForInMemory()))
            {
                LiteDbRepository<MsAuthor> authors = lite.CreateRepository<MsAuthor>();
                LiteDbRepository<MsBook> books = lite.CreateRepository<MsBook>();
                LiteDbRepository<MsAuthorTag> links = lite.CreateRepository<MsAuthorTag>();
                await SeedLibraryAsync(authors, books, lite.CreateRepository<MsTag>(), links);
                await AssertLibraryAsync(authors, books, links);
                await AssertDocumentRulesAsync(lite.CreateRepository<MsDocument>());
            }
        }

        /// <summary>
        /// A source set as <see cref="DurableMapping.MappingSource"/> maps the types it describes and leaves every other
        /// entity and projection type to its attributes and conventions.
        /// </summary>
        [Fact]
        public async Task GlobalSourceMapsOnlyTheTypesItDescribes()
        {
            IEntityMappingSource? previous = DurableMapping.MappingSource;
            DurableMapping.MappingSource = MsMappingRegistration.GlobalSource;
            try
            {
                EntityMetadata global = EntityMetadata.For<MsGlobalItem>();
                Assert.Same(MsMappingRegistration.GlobalSource, global.MappingSource);
                Assert.Equal("ms_global_items", global.TableName);
                Assert.Same(DurableMapping.AttributeSource, EntityMetadata.For<PersonNameDto>().MappingSource);
                Assert.Same(DurableMapping.AttributeSource, DurableMapping.GetMappingSource(typeof(MsLateItem)));
                Assert.Same(MsMappingRegistration.Source, DurableMapping.GetMappingSource(typeof(MsAuthor)));

                ISqlRepository<MsGlobalItem> items = _Provider.CreateRepository<MsGlobalItem>();
                await RelTestHelpers.RecreateTableAsync(items);
                MsGlobalItem created = await items.CreateAsync(new MsGlobalItem { Label = "one" });
                Assert.Equal("one", (await items.ReadByIdAsync(created.Id))!.Label);
            }
            finally
            {
                DurableMapping.MappingSource = previous;
            }
        }

        /// <summary>
        /// Once a type's metadata is built, registering a different source for it (per type or globally) throws and names the
        /// type; registering the source it was built with again is allowed. Null arguments are rejected.
        /// </summary>
        [Fact]
        public void MappingCannotChangeAfterMetadataIsBuilt()
        {
            EntityMetadata late = EntityMetadata.For<MsLateItem>();
            Assert.Same(DurableMapping.AttributeSource, late.MappingSource);
            Assert.Equal("MsLateItem", late.TableName);

            InvalidOperationException perType = Assert.Throws<InvalidOperationException>(() => DurableMapping.Register<MsLateItem>(MsMappingRegistration.Source));
            Assert.Contains(typeof(MsLateItem).FullName!, perType.Message, StringComparison.Ordinal);

            IEntityMappingSource? previous = DurableMapping.MappingSource;
            InvalidOperationException global = Assert.Throws<InvalidOperationException>(() => DurableMapping.MappingSource = new MapAttributeMappingSource(typeof(MsLateItem)));
            Assert.Contains(typeof(MsLateItem).FullName!, global.Message, StringComparison.Ordinal);
            Assert.Same(previous, DurableMapping.MappingSource);

            DurableMapping.Register<MsLateItem>(DurableMapping.AttributeSource);
            DurableMapping.Register<MsAuthor>(MsMappingRegistration.Source);
            EntityMetadata.For<MsAuthor>();
            DurableMapping.Register<MsAuthor>(MsMappingRegistration.Source);

            Assert.Throws<ArgumentNullException>(() => DurableMapping.Register<MsLateItem>(null!));
            Assert.Throws<ArgumentNullException>(() => DurableMapping.Register(null!, MsMappingRegistration.Source));
            Assert.Throws<ArgumentNullException>(() => DurableMapping.GetMappingSource(null!));
        }

        #endregion

        #region Private-Methods

        private static async Task SeedLibraryAsync(IRepository<MsAuthor> authors, IRepository<MsBook> books, IRepository<MsTag> tags, IRepository<MsAuthorTag> links)
        {
            MsAuthor austen = await authors.CreateAsync(new MsAuthor { Name = "Austen" });
            MsAuthor banks = await authors.CreateAsync(new MsAuthor { Name = "Banks" });
            await authors.CreateAsync(new MsAuthor { Name = "Christie" });
            await books.CreateAsync(new MsBook { Title = "Emma", AuthorId = austen.Id, Price = new Money(900) });
            await books.CreateAsync(new MsBook { Title = "Persuasion", AuthorId = austen.Id, Price = new Money(1100) });
            await books.CreateAsync(new MsBook { Title = "Excession", AuthorId = banks.Id, Price = new Money(1500) });
            MsTag classic = await tags.CreateAsync(new MsTag { Label = "Classic" });
            MsTag scifi = await tags.CreateAsync(new MsTag { Label = "SciFi" });
            await links.CreateAsync(new MsAuthorTag { AuthorId = austen.Id, TagId = classic.Id });
            await links.CreateAsync(new MsAuthorTag { AuthorId = banks.Id, TagId = classic.Id });
            await links.CreateAsync(new MsAuthorTag { AuthorId = banks.Id, TagId = scifi.Id });
        }

        private static async Task AssertLibraryAsync(IRepository<MsAuthor> authors, IRepository<MsBook> books, IRepository<MsAuthorTag> links)
        {
            List<MsBook> withAuthors = (await books.Query().Include(b => b.Author).OrderBy(b => b.Title).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Austen", "Banks", "Austen" }, withAuthors.Select(b => b.Author!.Name).ToArray());
            Assert.Equal(new Money(900), withAuthors[0].Price);

            List<MsAuthor> loaded = (await authors.Query().Include(a => a.Books).Include(a => a.Tags).OrderBy(a => a.Name).ExecuteAsync()).ToList();
            Assert.Equal(new[] { "Emma", "Persuasion" }, loaded[0].Books.Select(b => b.Title).OrderBy(t => t).ToArray());
            Assert.Equal(new[] { "Classic", "SciFi" }, loaded[1].Tags.Select(t => t.Label).OrderBy(l => l).ToArray());
            Assert.Empty(loaded[2].Books);
            Assert.Empty(loaded[2].Tags);

            MsAuthorTag? link = await links.ReadByIdAsync(new object[] { loaded[1].Id, loaded[1].Tags.Single(t => t.Label == "SciFi").Id });
            Assert.NotNull(link);
            Assert.Null(await links.ReadByIdAsync(new object[] { loaded[2].Id, loaded[1].Tags[0].Id }));
            Assert.True(await links.DeleteByIdAsync(new object[] { link!.AuthorId, link.TagId }));
            Assert.Equal(2, await links.CountAsync());
        }

        private static async Task AssertDocumentRulesAsync(IRepository<MsDocument> documents)
        {
            MsDocument created = await documents.CreateAsync(new MsDocument { Owner = "ann", Title = "Plan" });
            Assert.NotEqual(Guid.Empty, created.PublicId);
            await documents.CreateAsync(new MsDocument { Owner = "ann", Title = "Notes" });

            MsDocument first = (await documents.ReadByIdAsync(created.Id))!;
            MsDocument second = (await documents.ReadByIdAsync(created.Id))!;
            first.Title = "Plan v2";
            MsDocument updated = await documents.UpdateAsync(first);
            Assert.True(updated.Revision > second.Revision);
            second.Title = "Stale";
            await Assert.ThrowsAsync<OptimisticConcurrencyException>(() => documents.UpdateAsync(second));

            Assert.True(await documents.DeleteAsync(updated));
            Assert.Null(await documents.ReadByIdAsync(created.Id));
            Assert.Equal(1, await documents.CountAsync());
            MsDocument hidden = Assert.Single(await documents.Query().IgnoreQueryFilters().Where(d => d.Id == created.Id).ExecuteAsync());
            Assert.True(hidden.Deleted);
        }

        #endregion
    }
}
