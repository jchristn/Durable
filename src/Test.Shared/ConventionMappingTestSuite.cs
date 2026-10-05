namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Convention mapping coverage: entities without Entity/Property attributes (Id key, scalar properties,
    /// [NotMapped] and non-scalar exclusions) created via InitializeTable, plus the global snake-case naming
    /// convention applied to a dedicated, never-before-used entity type.
    /// </summary>
    public class ConventionMappingTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="ConventionMappingTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public ConventionMappingTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Convention metadata: type-name table, Id auto-increment key, scalar columns only, NotMapped excluded.
        /// </summary>
        [Fact]
        public void ConventionMetadataIsInferred()
        {
            EntityMetadata metadata = EntityMetadata.For<RelConventionWidget>();
            Assert.True(metadata.IsConventionMapped);
            Assert.False(metadata.HasEntityAttribute);
            Assert.Equal("RelConventionWidget", metadata.TableName);
            ColumnMetadata key = Assert.Single(metadata.KeyColumns);
            Assert.Equal("Id", key.Property.Name);
            Assert.True(key.IsAutoIncrement);
            Assert.Null(metadata.FindColumnByProperty("Computed"));
            Assert.Null(metadata.FindColumnByProperty("Labels"));
            Assert.Equal(new[] { "Id", "Name", "Quantity", "Price", "CreatedUtc", "Notes" }, metadata.Columns.Select(c => c.Property.Name).ToArray());
            Assert.True(metadata.FindColumnByProperty("Notes")?.IsNullable);
            Assert.False(metadata.FindColumnByProperty("Name")?.IsNullable);
        }

        /// <summary>
        /// A convention-mapped entity supports InitializeTable and full CRUD; NotMapped values are not persisted.
        /// </summary>
        [Fact]
        public async Task ConventionEntityCrud()
        {
            ISqlRepository<RelConventionWidget> repository = _Provider.CreateRepository<RelConventionWidget>();
            await RelTestHelpers.RecreateTableAsync(repository);

            DateTime created = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
            RelConventionWidget widget = await repository.CreateAsync(new RelConventionWidget
            {
                Name = "Sprocket",
                Quantity = 3,
                Price = 12.5m,
                CreatedUtc = created,
                Notes = null,
                Computed = "not stored",
                Labels = new List<string> { "ignored" }
            });
            Assert.True(widget.Id > 0);

            RelConventionWidget? loaded = await repository.ReadByIdAsync(widget.Id);
            Assert.NotNull(loaded);
            Assert.Equal("Sprocket", loaded.Name);
            Assert.Equal(3, loaded.Quantity);
            Assert.Equal(12.5m, loaded.Price);
            Assert.Equal(created, DateTime.SpecifyKind(loaded.CreatedUtc, DateTimeKind.Utc));
            Assert.Null(loaded.Notes);
            Assert.Null(loaded.Computed);
            Assert.Empty(loaded.Labels);

            loaded.Notes = "now set";
            loaded.Quantity = 4;
            await repository.UpdateAsync(loaded);
            RelConventionWidget? updated = await repository.ReadFirstAsync(w => w.Name == "Sprocket");
            Assert.NotNull(updated);
            Assert.Equal("now set", updated.Notes);
            Assert.Equal(4, updated.Quantity);

            RelConventionWidget second = repository.Create(new RelConventionWidget { Name = "Gear", Quantity = 1, Price = 1m, CreatedUtc = created });
            Assert.NotEqual(widget.Id, second.Id);
            Assert.Equal(2, await repository.CountAsync());
            Assert.True(await repository.DeleteByIdAsync(widget.Id));
            Assert.Equal(1, await repository.CountAsync());
            Assert.False(await repository.ExistsByIdAsync(widget.Id));
        }

        /// <summary>
        /// With the snake-case naming convention active when metadata is first built, table and column names are
        /// snake-cased in DDL and DML. The global setting is restored afterwards.
        /// </summary>
        [Fact]
        public async Task SnakeCaseNamingConventionAppliesToTableAndColumns()
        {
            NamingConvention original = DurableMapping.NamingConvention;
            EntityMetadata metadata;
            try
            {
                DurableMapping.NamingConvention = NamingConvention.SnakeCase;
                metadata = EntityMetadata.For<RelSnakeCaseGadget>();
            }
            finally
            {
                DurableMapping.NamingConvention = original;
            }

            Assert.Equal("rel_snake_case_gadget", metadata.TableName);
            Assert.Equal(new[] { "id", "display_name", "unit_price", "is_active", "created_utc" }, metadata.Columns.Select(c => c.Name).ToArray());

            ISqlRepository<RelSnakeCaseGadget> repository = _Provider.CreateRepository<RelSnakeCaseGadget>();
            await RelTestHelpers.RecreateTableAsync(repository);

            RelSnakeCaseGadget gadget = await repository.CreateAsync(new RelSnakeCaseGadget
            {
                DisplayName = "Snake",
                UnitPrice = 3.25m,
                IsActive = true,
                CreatedUtc = DateTime.UtcNow
            });

            string? displayName = await repository.ExecuteScalarAsync<string>("SELECT display_name FROM rel_snake_case_gadget WHERE id = @p0", null, default, gadget.Id);
            Assert.Equal("Snake", displayName);

            RelSnakeCaseGadget? loaded = await repository.ReadFirstAsync(g => g.DisplayName == "Snake" && g.IsActive);
            Assert.NotNull(loaded);
            Assert.Equal(3.25m, loaded.UnitPrice);

            await repository.UpdateFieldAsync(g => g.Id == gadget.Id, g => g.IsActive, false);
            Assert.Equal(0, await repository.CountAsync(g => g.IsActive));
            Assert.Equal(1, await repository.CountAsync(g => !g.IsActive));
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion
    }
}
