namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Durable;

    /// <summary>
    /// Base class of the built-in suites: seeding helpers on top of <see cref="ConformanceSuite"/>.
    /// </summary>
    internal abstract class KitSuite : ConformanceSuite
    {
        protected KitSuite(IConformanceTarget target) : base(target)
        {
        }

        protected async Task<ItemFixture> SeedItemsAsync(RepositoryOptions? options = null)
        {
            await ResetAsync(typeof(CfItem), typeof(CfOwner)).ConfigureAwait(false);
            return await ItemFixture.CreateAsync(Repository<CfItem>(options), Repository<CfOwner>(options), Token).ConfigureAwait(false);
        }

        protected async Task<LibraryFixture> SeedLibraryAsync()
        {
            await ResetAsync(ConformanceEntities.Library.ToArray()).ConfigureAwait(false);
            return await LibraryFixture.CreateAsync(
                Repository<CfPublisher>(),
                Repository<CfAuthor>(),
                Repository<CfBook>(),
                Repository<CfTag>(),
                Repository<CfAuthorTag>(),
                Repository<CfNote>(),
                Token).ConfigureAwait(false);
        }

        protected Task CheckAsync(ItemFixture fixture, Expression<Func<CfItem, bool>> predicate, params string[] expected)
        {
            return fixture.CheckAsync(predicate, expected, Token);
        }

        protected async Task<List<T>> ExecuteAsync<T>(IQueryBuilder<T> query, string context) where T : class, new()
        {
            try
            {
                return (await query.ExecuteAsync(Token).ConfigureAwait(false)).ToList();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(context + " failed on " + Target.Name + ": " + ex.GetType().Name + ": " + ex.Message, ex);
            }
        }

        protected async Task AssertNamesAsync(IQueryBuilder<CfItem> query, string context, params string[] expected)
        {
            List<CfItem> rows = await ExecuteAsync(query, context).ConfigureAwait(false);
            ConformanceAssert.NameSet(rows.Select(r => r.Name), expected, context);
        }

        protected async Task AssertSequenceAsync(IQueryBuilder<CfItem> query, string context, params string[] expected)
        {
            List<CfItem> rows = await ExecuteAsync(query, context).ConfigureAwait(false);
            ConformanceAssert.Sequence(rows.Select(r => r.Name), expected, context);
        }
    }
}
