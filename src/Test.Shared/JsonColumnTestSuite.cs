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
    /// JSON column coverage: an explicit <see cref="Flags.Json"/> object, and implicit JSON for a nested object, a
    /// list and a dictionary. Tables are created with InitializeTable so each provider's JSON column type is exercised.
    /// </summary>
    public class JsonColumnTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonColumnTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public JsonColumnTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Metadata marks explicit and implicit JSON columns as JSON.
        /// </summary>
        [Fact]
        public void JsonColumnsAreDetected()
        {
            EntityMetadata metadata = EntityMetadata.For<RelJsonDocument>();
            Assert.True(metadata.FindColumnByName("payload")?.IsJson);
            Assert.True(metadata.FindColumnByName("nested")?.IsJson);
            Assert.True(metadata.FindColumnByName("tags")?.IsJson);
            Assert.True(metadata.FindColumnByName("counters")?.IsJson);
            Assert.False(metadata.FindColumnByName("name")?.IsJson);
        }

        /// <summary>
        /// Explicit and implicit JSON values, including nested children, round-trip.
        /// </summary>
        [Fact]
        public async Task JsonValuesRoundTrip()
        {
            ISqlRepository<RelJsonDocument> repository = await PrepareAsync();
            RelJsonDocument created = await repository.CreateAsync(new RelJsonDocument
            {
                Name = "doc",
                Payload = new RelJsonPayload
                {
                    Label = "explicit",
                    Score = 7,
                    Values = new List<string> { "a", "b" },
                    Child = new RelJsonPayload { Label = "child", Score = 8, Values = new List<string> { "c" } }
                },
                Nested = new RelJsonPayload { Label = "implicit", Score = 9 },
                Tags = new List<string> { "t1", "t2", "t3" },
                Counters = new Dictionary<string, int> { { "views", 10 }, { "likes", 3 } }
            });

            RelJsonDocument? loaded = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(loaded);
            Assert.NotNull(loaded.Payload);
            Assert.Equal("explicit", loaded.Payload.Label);
            Assert.Equal(7, loaded.Payload.Score);
            Assert.Equal(new[] { "a", "b" }, loaded.Payload.Values.ToArray());
            Assert.NotNull(loaded.Payload.Child);
            Assert.Equal("child", loaded.Payload.Child.Label);
            Assert.Equal(new[] { "c" }, loaded.Payload.Child.Values.ToArray());
            Assert.NotNull(loaded.Nested);
            Assert.Equal("implicit", loaded.Nested.Label);
            Assert.Equal(9, loaded.Nested.Score);
            Assert.Equal(new[] { "t1", "t2", "t3" }, loaded.Tags?.ToArray());
            Assert.NotNull(loaded.Counters);
            Assert.Equal(10, loaded.Counters["views"]);
            Assert.Equal(3, loaded.Counters["likes"]);

            string? raw = await repository.ExecuteScalarAsync<string>("SELECT payload FROM rel_json_documents WHERE id = @p0", null, default, created.Id);
            Assert.NotNull(raw);
            Assert.Contains("explicit", raw);
            Assert.Contains("child", raw);
        }

        /// <summary>
        /// Null JSON values round-trip as null; CreateMany and Update handle JSON columns.
        /// </summary>
        [Fact]
        public async Task JsonNullsCreateManyAndUpdate()
        {
            ISqlRepository<RelJsonDocument> repository = await PrepareAsync();
            List<RelJsonDocument> created = (await repository.CreateManyAsync(new List<RelJsonDocument>
            {
                new RelJsonDocument { Name = "empty" },
                new RelJsonDocument { Name = "full", Tags = new List<string> { "x" }, Payload = new RelJsonPayload { Label = "p" } }
            })).ToList();

            RelJsonDocument? empty = await repository.ReadByIdAsync(created[0].Id);
            Assert.NotNull(empty);
            Assert.Null(empty.Payload);
            Assert.Null(empty.Nested);
            Assert.Null(empty.Tags);
            Assert.Null(empty.Counters);

            RelJsonDocument? full = await repository.ReadByIdAsync(created[1].Id);
            Assert.NotNull(full);
            Assert.Equal("p", full.Payload?.Label);
            full.Tags = new List<string> { "x", "y" };
            full.Payload = null;
            full.Counters = new Dictionary<string, int> { { "n", 1 } };
            await repository.UpdateAsync(full);

            RelJsonDocument? reloaded = await repository.ReadByIdAsync(created[1].Id);
            Assert.NotNull(reloaded);
            Assert.Equal(new[] { "x", "y" }, reloaded.Tags?.ToArray());
            Assert.Null(reloaded.Payload);
            Assert.Equal(1, reloaded.Counters?["n"]);
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<RelJsonDocument>> PrepareAsync()
        {
            ISqlRepository<RelJsonDocument> repository = _Provider.CreateRepository<RelJsonDocument>();
            await RelTestHelpers.RecreateTableAsync(repository);
            return repository;
        }

        #endregion
    }
}
