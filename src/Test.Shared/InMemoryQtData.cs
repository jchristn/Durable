namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Durable;
    using Durable.InMemory;

    /// <summary>
    /// The query-translation data set (six <see cref="QtItem"/>s with tricky values and four <see cref="QtOwner"/>s, the
    /// same rows as <see cref="QueryTranslationFixture"/>) seeded into a fresh <see cref="InMemoryBackend"/>.
    /// </summary>
    public sealed class InMemoryQtData
    {
        #region Public-Members

        /// <summary>Name of the first item.</summary>
        public const string Alpha = QueryTranslationFixture.Alpha;

        /// <summary>Name of the second item.</summary>
        public const string Beta = QueryTranslationFixture.Beta;

        /// <summary>Name containing an apostrophe.</summary>
        public const string OBrien = QueryTranslationFixture.OBrien;

        /// <summary>Name with a diacritic.</summary>
        public const string Zoe = QueryTranslationFixture.Zoe;

        /// <summary>Non-Latin name.</summary>
        public const string Nihon = QueryTranslationFixture.Nihon;

        /// <summary>Name with surrounding spaces.</summary>
        public const string Padded = QueryTranslationFixture.Padded;

        /// <summary>
        /// Gets the backend. Never null.
        /// </summary>
        public InMemoryBackend Backend { get; }

        /// <summary>
        /// Gets the item repository. Never null.
        /// </summary>
        public InMemoryRepository<QtItem> Items { get; }

        /// <summary>
        /// Gets the owner repository. Never null.
        /// </summary>
        public InMemoryRepository<QtOwner> Owners { get; }

        /// <summary>
        /// Gets the seeded owners by name. Never null.
        /// </summary>
        public Dictionary<string, QtOwner> SeededOwners { get; } = new Dictionary<string, QtOwner>(StringComparer.Ordinal);

        /// <summary>
        /// Gets the seeded items in insertion order. Never null.
        /// </summary>
        public List<QtItem> SeededItems { get; } = new List<QtItem>();

        #endregion

        #region Constructors-and-Factories

        private InMemoryQtData(RepositoryCapabilities capabilities, RepositoryOptions? options)
        {
            Backend = new InMemoryBackend(capabilities);
            Items = Backend.CreateRepository<QtItem>(options);
            Owners = Backend.CreateRepository<QtOwner>(options);
        }

        /// <summary>
        /// Creates and seeds a data set.
        /// </summary>
        /// <param name="capabilities">Backend capabilities. Default: all.</param>
        /// <param name="options">Repository options; null for defaults.</param>
        /// <returns>The seeded data set.</returns>
        public static async Task<InMemoryQtData> CreateAsync(RepositoryCapabilities capabilities = RepositoryCapabilities.All, RepositoryOptions? options = null)
        {
            InMemoryQtData data = new InMemoryQtData(capabilities, options);
            await data.SeedAsync().ConfigureAwait(false);
            return data;
        }

        #endregion

        #region Private-Methods

        private async Task SeedAsync()
        {
            foreach (QtOwner owner in new[]
            {
                new QtOwner { Name = "Acme", City = "Berlin" },
                new QtOwner { Name = "Globex", City = null },
                new QtOwner { Name = "Initech", City = "Austin" },
                new QtOwner { Name = "Umbrella", City = "Raccoon City" }
            })
            {
                QtOwner created = await Owners.CreateAsync(owner).ConfigureAwait(false);
                SeededOwners[created.Name] = created;
            }

            List<QtItem> items = new List<QtItem>
            {
                new QtItem
                {
                    Name = Alpha, Code = "A-100", Email = "alpha@x.com", Status = QtStatus.Active, Priority = QtPriority.High,
                    Price = 10.50m, Ratio = 0.25, Quantity = 5, Discount = 2, IsActive = true, IsFeatured = true,
                    CreatedUtc = new DateTime(2024, 3, 15, 10, 30, 0), DueDate = new DateTime(2024, 4, 1),
                    Category = "Tools", OwnerId = SeededOwners["Acme"].Id
                },
                new QtItem
                {
                    Name = Beta, Code = "50%_off", Email = null, Status = QtStatus.Draft, Priority = QtPriority.Low,
                    Price = 20.00m, Ratio = 1.5, Quantity = 0, Discount = null, IsActive = false, IsFeatured = null,
                    CreatedUtc = new DateTime(2023, 12, 31, 23, 0, 0), DueDate = null,
                    Category = "Tools", OwnerId = SeededOwners["Acme"].Id
                },
                new QtItem
                {
                    Name = OBrien, Code = "path\\to\\file", Email = "OBRIEN@X.COM", Status = QtStatus.Suspended, Priority = QtPriority.Critical,
                    Price = 7.25m, Ratio = 2.75, Quantity = 12, Discount = 0, IsActive = true, IsFeatured = false,
                    CreatedUtc = new DateTime(2024, 1, 1, 0, 0, 0), DueDate = new DateTime(2024, 1, 10),
                    Category = "Garden", OwnerId = SeededOwners["Globex"].Id
                },
                new QtItem
                {
                    Name = Zoe, Code = "[draft]!", Email = "zoe@x.com", Status = QtStatus.Closed, Priority = QtPriority.Medium,
                    Price = 99.99m, Ratio = 0.5, Quantity = 3, Discount = 5, IsActive = true, IsFeatured = true,
                    CreatedUtc = new DateTime(2024, 2, 29, 8, 15, 0), DueDate = new DateTime(2024, 3, 5),
                    Category = "Garden", OwnerId = SeededOwners["Globex"].Id
                },
                new QtItem
                {
                    Name = Nihon, Code = "line1\nline2", Email = string.Empty, Status = QtStatus.Active, Priority = QtPriority.Medium,
                    Price = 15.00m, Ratio = -1.25, Quantity = 8, Discount = null, IsActive = false, IsFeatured = null,
                    CreatedUtc = new DateTime(2024, 7, 4, 12, 0, 0), DueDate = null,
                    Category = "Kitchen", OwnerId = null
                },
                new QtItem
                {
                    Name = Padded, Code = "100%", Email = "   ", Status = QtStatus.Active, Priority = QtPriority.Low,
                    Price = 0.10m, Ratio = 0.1, Quantity = 1, Discount = 1, IsActive = true, IsFeatured = false,
                    CreatedUtc = new DateTime(2024, 3, 16, 0, 0, 0), DueDate = new DateTime(2024, 3, 20),
                    Category = "Kitchen", OwnerId = SeededOwners["Initech"].Id
                }
            };

            foreach (QtItem item in items) SeededItems.Add(await Items.CreateAsync(item).ConfigureAwait(false));
        }

        #endregion
    }
}
