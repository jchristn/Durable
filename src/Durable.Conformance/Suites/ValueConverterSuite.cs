namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Xunit;

    /// <summary>
    /// Per-property value converters (always required): converted values round-trip through create, create-many and
    /// update, and predicate values are converted the same way (struct, list and enum converters); set-based updates apply
    /// converters (<see cref="RepositoryCapabilities.BatchUpdate"/>). Also convention-mapped entities (no attributes).
    /// </summary>
    internal sealed class ValueConverterSuite : KitSuite
    {
        public ValueConverterSuite(IConformanceTarget target) : base(target)
        {
        }

        [ConformanceTest(Description = "Struct, list and enum converters round-trip")]
        public async Task ConvertedValuesRoundTrip()
        {
            await ResetAsync(typeof(CfConvertedItem));
            IRepository<CfConvertedItem> repository = Repository<CfConvertedItem>();
            CfConvertedItem created = await repository.CreateAsync(new CfConvertedItem
            {
                Name = "Widget",
                Price = new CfMoney(1999),
                Tags = new List<string> { "red", "large", "sale" },
                Grade = CfGrade.High
            }, null, Token);
            CfConvertedItem? loaded = await repository.ReadByIdAsync(created.Id, null, Token);
            Assert.NotNull(loaded);
            Assert.Equal(new CfMoney(1999), loaded.Price);
            Assert.Equal(new[] { "red", "large", "sale" }, loaded.Tags.ToArray());
            Assert.Equal(CfGrade.High, loaded.Grade);
        }

        [ConformanceTest(Description = "Converted values survive CreateMany and Update (including an empty list)")]
        public async Task ConvertedValuesSurviveCreateManyAndUpdate()
        {
            IRepository<CfConvertedItem> repository = await SeedAsync();
            CfConvertedItem three = repository.ReadSingle(x => x.Name == "three");
            Assert.Empty(three.Tags);
            Assert.Equal(CfGrade.High, three.Grade);
            three.Price = new CfMoney(250);
            three.Tags = new List<string> { "y", "z" };
            three.Grade = CfGrade.Low;
            await repository.UpdateAsync(three, null, Token);
            CfConvertedItem? reloaded = repository.ReadById(three.Id);
            Assert.NotNull(reloaded);
            Assert.Equal(new CfMoney(250), reloaded.Price);
            Assert.Equal(new[] { "y", "z" }, reloaded.Tags.ToArray());
            Assert.Equal(CfGrade.Low, reloaded.Grade);
        }

        [ConformanceTest(Description = "Predicates on a converted enum convert the compared value")]
        public async Task PredicateOnConvertedEnum()
        {
            IRepository<CfConvertedItem> repository = await SeedAsync();
            CfGrade wanted = CfGrade.Low;
            ConformanceAssert.NameSet(repository.ReadMany(x => x.Grade == CfGrade.High).Select(x => x.Name), new[] { "one", "three" }, "Grade == High");
            Assert.Equal(1L, await repository.CountAsync(x => x.Grade == wanted, null, Token));
            Assert.Equal(3L, repository.Count(x => x.Grade != CfGrade.Low));
        }

        [ConformanceTest(Description = "Predicates on a converted struct convert the compared value")]
        public async Task PredicateOnConvertedStruct()
        {
            IRepository<CfConvertedItem> repository = await SeedAsync();
            CfMoney price = new CfMoney(500);
            ConformanceAssert.NameSet(repository.ReadMany(x => x.Price == price).Select(x => x.Name), new[] { "one", "two" }, "Price == 500c");
            Assert.True(await repository.ExistsAsync(x => x.Price == new CfMoney(1500), null, Token));
            Assert.False(repository.Exists(x => x.Price == new CfMoney(1)));
        }

        [ConformanceTest(Description = "Predicates on a converted list convert the compared value")]
        public async Task PredicateOnConvertedList()
        {
            IRepository<CfConvertedItem> repository = await SeedAsync();
            List<string> tags = new List<string> { "a", "b" };
            Assert.Equal("one", Assert.Single(await repository.Query().Where(x => x.Tags == tags).ExecuteAsync(Token)).Name);
        }

        [ConformanceTest(Requires = RepositoryCapabilities.BatchUpdate, Description = "UpdateField and BatchUpdate apply converters")]
        public async Task SetBasedUpdatesApplyConverters()
        {
            IRepository<CfConvertedItem> repository = await SeedAsync();
            Assert.Equal(1, await repository.UpdateFieldAsync(x => x.Name == "one", x => x.Grade, CfGrade.Low, null, Token));
            Assert.Equal(CfGrade.Low, repository.ReadSingle(x => x.Name == "one").Grade);
            Assert.Equal(2L, repository.Count(x => x.Grade == CfGrade.Low));
            Assert.Equal(1, await repository.BatchUpdateAsync(x => x.Name == "two", x => new CfConvertedItem { Price = new CfMoney(42), Grade = CfGrade.Medium }, null, Token));
            CfConvertedItem two = repository.ReadSingle(x => x.Name == "two");
            Assert.Equal(new CfMoney(42), two.Price);
            Assert.Equal(CfGrade.Medium, two.Grade);
            Assert.True(repository.Exists(x => x.Price == new CfMoney(42)));
        }

        [ConformanceTest(Description = "Convention mapping infers storage name, key and columns; NotMapped is ignored")]
        public void ConventionMetadata()
        {
            EntityMetadata metadata = Repository<CfConventionWidget>().Metadata;
            Assert.True(metadata.IsConventionMapped);
            Assert.Equal("CfConventionWidget", metadata.TableName);
            ColumnMetadata key = Assert.Single(metadata.KeyColumns);
            Assert.Equal("Id", key.Property.Name);
            Assert.True(key.IsAutoIncrement);
            Assert.Null(metadata.FindColumnByProperty("Computed"));
        }

        [ConformanceTest(Description = "Convention-mapped entity CRUD")]
        public async Task ConventionEntityCrud()
        {
            await ResetAsync(typeof(CfConventionWidget));
            IRepository<CfConventionWidget> repository = Repository<CfConventionWidget>();
            CfConventionWidget widget = await repository.CreateAsync(new CfConventionWidget { Name = "Sprocket", Quantity = 3, Price = 12.5m, Computed = "not stored" }, null, Token);
            Assert.True(widget.Id > 0);
            CfConventionWidget? loaded = await repository.ReadByIdAsync(widget.Id, null, Token);
            Assert.NotNull(loaded);
            Assert.Equal("Sprocket", loaded.Name);
            Assert.Equal(12.5m, loaded.Price);
            Assert.Null(loaded.Notes);
            Assert.Null(loaded.Computed);
            loaded.Notes = "now set";
            loaded.Quantity = 4;
            repository.Update(loaded);
            CfConventionWidget? updated = repository.ReadFirst(w => w.Name == "Sprocket");
            Assert.Equal("now set", updated?.Notes);
            Assert.Equal(4, updated?.Quantity);
            CfConventionWidget second = repository.Create(new CfConventionWidget { Name = "Gear", Quantity = 1, Price = 1m });
            Assert.NotEqual(widget.Id, second.Id);
            Assert.Equal(2L, repository.Count());
            Assert.True(await repository.DeleteByIdAsync(widget.Id, null, Token));
            Assert.Equal(1L, repository.Count());
        }

        private async Task<IRepository<CfConvertedItem>> SeedAsync()
        {
            await ResetAsync(typeof(CfConvertedItem));
            IRepository<CfConvertedItem> repository = Repository<CfConvertedItem>();
            await repository.CreateManyAsync(new List<CfConvertedItem>
            {
                new CfConvertedItem { Name = "one", Price = new CfMoney(500), Tags = new List<string> { "a", "b" }, Grade = CfGrade.High },
                new CfConvertedItem { Name = "two", Price = new CfMoney(500), Tags = new List<string> { "c" }, Grade = CfGrade.Medium },
                new CfConvertedItem { Name = "three", Price = new CfMoney(1500), Tags = new List<string>(), Grade = CfGrade.High },
                new CfConvertedItem { Name = "four", Price = new CfMoney(2500), Tags = new List<string> { "a" }, Grade = CfGrade.Low }
            }, null, Token);
            return repository;
        }
    }
}
