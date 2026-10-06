namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.ConcurrencyConflictResolvers;
    using Durable.DefaultValueProviders;
    using Xunit;

    /// <summary>
    /// Calls every built-in conflict resolver and default-value provider directly (no database): sync and async resolve
    /// and try-resolve forms, cancellation, merge behaviors, and provider ShouldApply/GetDefaultValue rules.
    /// </summary>
    public class ResolverAndDefaultValueTestSuite
    {
        #region Public-Methods

        /// <summary>
        /// ClientWins returns the incoming entity and DatabaseWins the current one from all four methods.
        /// </summary>
        [Fact]
        public async Task ClientAndDatabaseWinsResolveDirectly()
        {
            ResolverSample current = Sample(1, "current", 1);
            ResolverSample incoming = Sample(1, "incoming", 2);
            ResolverSample original = Sample(1, "original", 0);

            ClientWinsResolver<ResolverSample> client = new ClientWinsResolver<ResolverSample>();
            Assert.Equal(ConflictResolutionStrategy.ClientWins, client.DefaultStrategy);
            Assert.Same(incoming, client.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.ClientWins));
            Assert.Same(incoming, await client.ResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.ClientWins));
            Assert.True(client.TryResolveConflict(current, incoming, original, ConflictResolutionStrategy.ClientWins, out ResolverSample clientResult));
            Assert.Same(incoming, clientResult);
            TryResolveConflictResult<ResolverSample> clientTry = await client.TryResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.ClientWins);
            Assert.True(clientTry.Success);
            Assert.Same(incoming, clientTry.ResolvedEntity);

            DatabaseWinsResolver<ResolverSample> database = new DatabaseWinsResolver<ResolverSample>();
            Assert.Equal(ConflictResolutionStrategy.DatabaseWins, database.DefaultStrategy);
            Assert.Same(current, database.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.DatabaseWins));
            Assert.Same(current, await database.ResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.DatabaseWins));
            Assert.True(database.TryResolveConflict(current, incoming, original, ConflictResolutionStrategy.DatabaseWins, out ResolverSample databaseResult));
            Assert.Same(current, databaseResult);
            Assert.Same(current, (await database.TryResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.DatabaseWins)).ResolvedEntity);
        }

        /// <summary>
        /// ThrowExceptionResolver throws ConcurrencyConflictException (carrying the entities) from the resolve methods
        /// and reports failure from the try methods.
        /// </summary>
        [Fact]
        public async Task ThrowExceptionResolverNeverResolves()
        {
            ResolverSample current = Sample(1, "current", 1);
            ResolverSample incoming = Sample(1, "incoming", 2);
            ResolverSample original = Sample(1, "original", 0);
            ThrowExceptionResolver<ResolverSample> resolver = new ThrowExceptionResolver<ResolverSample>();
            Assert.Equal(ConflictResolutionStrategy.ThrowException, resolver.DefaultStrategy);

            ConcurrencyConflictException sync = Assert.Throws<ConcurrencyConflictException>(() => resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.ThrowException));
            Assert.Same(current, sync.CurrentEntity);
            Assert.Same(incoming, sync.IncomingEntity);
            Assert.Same(original, sync.OriginalEntity);
            await Assert.ThrowsAsync<ConcurrencyConflictException>(() => resolver.ResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.ThrowException));

            Assert.False(resolver.TryResolveConflict(current, incoming, original, ConflictResolutionStrategy.ThrowException, out ResolverSample resolved));
            Assert.Null(resolved);
            TryResolveConflictResult<ResolverSample> attempt = await resolver.TryResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.ThrowException);
            Assert.False(attempt.Success);
            Assert.Null(attempt.ResolvedEntity);
        }

        /// <summary>
        /// MergeChangesResolver keeps one-sided changes, resolves two-sided changes by its behavior, compares collections
        /// element-wise, keeps ignored properties from the stored entity, and validates arguments.
        /// </summary>
        [Fact]
        public async Task MergeChangesResolverBehaviors()
        {
            ResolverSample original = Sample(1, "orig", 1, "a");
            ResolverSample current = Sample(1, "orig", 5, "a");      // stored side changed Count
            ResolverSample incoming = Sample(1, "renamed", 7, "a");  // caller changed Name and Count

            MergeChangesResolver<ResolverSample> incomingWins = new MergeChangesResolver<ResolverSample>();
            Assert.Equal(MergeConflictBehavior.IncomingWins, incomingWins.ConflictBehavior);
            ResolverSample merged = incomingWins.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            Assert.Equal("renamed", merged.Name);
            Assert.Equal(7, merged.Count);

            MergeChangesResolver<ResolverSample> currentWins = new MergeChangesResolver<ResolverSample>(MergeConflictBehavior.CurrentWins);
            ResolverSample mergedCurrent = await currentWins.ResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.MergeChanges);
            Assert.Equal("renamed", mergedCurrent.Name);
            Assert.Equal(5, mergedCurrent.Count);

            MergeChangesResolver<ResolverSample> throwing = new MergeChangesResolver<ResolverSample>(MergeConflictBehavior.ThrowException);
            ConcurrencyConflictException conflict = Assert.Throws<ConcurrencyConflictException>(() => throwing.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges));
            Assert.Contains("Count", conflict.Message);
            Assert.False(throwing.TryResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges, out ResolverSample failed));
            Assert.Null(failed);
            Assert.False((await throwing.TryResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.MergeChanges)).Success);

            // Equal lists in different instances are not a change; the stored side changing the list keeps it.
            ResolverSample listCurrent = Sample(1, "orig", 1, "a", "b");
            ResolverSample listIncoming = Sample(1, "orig", 9, "a");
            ResolverSample listMerged = incomingWins.ResolveConflict(listCurrent, listIncoming, original, ConflictResolutionStrategy.MergeChanges);
            Assert.Equal(new List<string> { "a", "b" }, listMerged.Tags);
            Assert.Equal(9, listMerged.Count);

            MergeChangesResolver<ResolverSample> ignoring = new MergeChangesResolver<ResolverSample>(nameof(ResolverSample.Name));
            Assert.Equal("orig", ignoring.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges).Name);

            Assert.Throws<ArgumentNullException>(() => incomingWins.ResolveConflict(null!, incoming, original, ConflictResolutionStrategy.MergeChanges));
            Assert.False(incomingWins.TryResolveConflict(current, null!, original, ConflictResolutionStrategy.MergeChanges, out _));
        }

        /// <summary>
        /// DefaultConflictResolver dispatches by strategy and uses DefaultStrategy for Custom.
        /// </summary>
        [Fact]
        public async Task DefaultConflictResolverDispatches()
        {
            ResolverSample current = Sample(1, "current", 1);
            ResolverSample incoming = Sample(1, "incoming", 2);
            ResolverSample original = Sample(1, "current", 0);
            DefaultConflictResolver<ResolverSample> resolver = new DefaultConflictResolver<ResolverSample>();
            Assert.Equal(ConflictResolutionStrategy.ThrowException, resolver.DefaultStrategy);

            Assert.Same(incoming, resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.ClientWins));
            Assert.Same(current, await resolver.ResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.DatabaseWins));
            Assert.Equal("incoming", resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.MergeChanges).Name);
            Assert.Throws<ConcurrencyConflictException>(() => resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.ThrowException));
            Assert.Throws<ConcurrencyConflictException>(() => resolver.ResolveConflict(current, incoming, original, ConflictResolutionStrategy.Custom));

            resolver.DefaultStrategy = ConflictResolutionStrategy.ClientWins;
            Assert.True(resolver.TryResolveConflict(current, incoming, original, ConflictResolutionStrategy.Custom, out ResolverSample custom));
            Assert.Same(incoming, custom);
            Assert.True((await resolver.TryResolveConflictAsync(current, incoming, original, ConflictResolutionStrategy.Custom)).Success);
        }

        /// <summary>
        /// The async resolver methods observe their CancellationToken.
        /// </summary>
        [Fact]
        public async Task ResolversHonorCancellation()
        {
            using CancellationTokenSource source = new CancellationTokenSource();
            source.Cancel();
            ResolverSample entity = Sample(1, "x", 1);
            List<IConcurrencyConflictResolver<ResolverSample>> resolvers = new List<IConcurrencyConflictResolver<ResolverSample>>
            {
                new ClientWinsResolver<ResolverSample>(),
                new DatabaseWinsResolver<ResolverSample>(),
                new ThrowExceptionResolver<ResolverSample>(),
                new MergeChangesResolver<ResolverSample>(),
                new DefaultConflictResolver<ResolverSample>(ConflictResolutionStrategy.ClientWins)
            };

            foreach (IConcurrencyConflictResolver<ResolverSample> resolver in resolvers)
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => resolver.ResolveConflictAsync(entity, entity, entity, resolver.DefaultStrategy, source.Token));
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => resolver.TryResolveConflictAsync(entity, entity, entity, resolver.DefaultStrategy, source.Token));
            }
        }

        /// <summary>
        /// The built-in default-value providers produce values and apply only to unset values (unless configured
        /// otherwise).
        /// </summary>
        [Fact]
        public void DefaultValueProvidersApplyRules()
        {
            PropertyInfo property = typeof(ResolverSample).GetProperty(nameof(ResolverSample.Name))!;
            ResolverSample entity = new ResolverSample();

            NewGuidProvider newGuid = new NewGuidProvider();
            Guid first = (Guid)newGuid.GetDefaultValue(property, entity)!;
            Assert.NotEqual(Guid.Empty, first);
            Assert.NotEqual(first, (Guid)newGuid.GetDefaultValue(property, entity)!);
            Assert.True(newGuid.ShouldApply(Guid.Empty, typeof(Guid)));
            Assert.True(newGuid.ShouldApply(null, typeof(Guid?)));
            Assert.False(newGuid.ShouldApply(first, typeof(Guid)));

            SequentialGuidProvider sequential = new SequentialGuidProvider();
            Guid a = (Guid)sequential.GetDefaultValue(property, entity)!;
            Thread.Sleep(2);
            Guid b = (Guid)sequential.GetDefaultValue(property, entity)!;
            Assert.NotEqual(a, b);
            Assert.True(string.CompareOrdinal(TimestampBytes(a), TimestampBytes(b)) < 0, "Sequential GUIDs must embed increasing timestamps.");
            Assert.True(sequential.ShouldApply(Guid.Empty, typeof(Guid)));
            Assert.False(sequential.ShouldApply(a, typeof(Guid)));

            CurrentDateTimeUtcProvider now = new CurrentDateTimeUtcProvider();
            DateTime value = (DateTime)now.GetDefaultValue(property, entity)!;
            Assert.Equal(DateTimeKind.Utc, value.Kind);
            Assert.True((DateTime.UtcNow - value).Duration() < TimeSpan.FromMinutes(1));
            Assert.True(now.ShouldApply(default(DateTime), typeof(DateTime)));
            Assert.False(now.ShouldApply(value, typeof(DateTime)));

            StaticValueProvider onlyIfNull = new StaticValueProvider(42);
            Assert.Equal(42, onlyIfNull.GetDefaultValue(property, entity));
            Assert.True(onlyIfNull.ShouldApply(0, typeof(int)));
            Assert.True(onlyIfNull.ShouldApply(null, typeof(string)));
            Assert.False(onlyIfNull.ShouldApply(7, typeof(int)));
            Assert.False(onlyIfNull.ShouldApply("set", typeof(string)));
            StaticValueProvider always = new StaticValueProvider("x", false);
            Assert.True(always.ShouldApply("set", typeof(string)));

            int calls = 0;
            DelegateValueProvider factory = new DelegateValueProvider(() => ++calls);
            Assert.Equal(1, factory.GetDefaultValue(property, entity));
            Assert.Equal(2, factory.GetDefaultValue(property, entity));
            Assert.True(factory.ShouldApply(null, typeof(string)));
            Assert.False(factory.ShouldApply("set", typeof(string)));
            Assert.True(new DelegateValueProvider(() => null, false).ShouldApply("set", typeof(string)));
        }

        /// <summary>
        /// DefaultValueAttribute's static-value and provider-type constructors record their configuration, and the
        /// provider-type form rejects types that are not providers.
        /// </summary>
        [Fact]
        public void DefaultValueAttributeConstructors()
        {
            DefaultValueAttribute staticValue = new DefaultValueAttribute("draft");
            Assert.Equal(DefaultValueType.StaticValue, staticValue.ValueType);
            Assert.Equal("draft", staticValue.StaticValue);
            Assert.True(staticValue.OnlyIfNull);

            DefaultValueAttribute provider = new DefaultValueAttribute(typeof(NewGuidProvider), false);
            Assert.Equal(DefaultValueType.CustomProvider, provider.ValueType);
            Assert.Equal(typeof(NewGuidProvider), provider.ProviderType);
            Assert.False(provider.OnlyIfNull);

            Assert.Throws<ArgumentException>(() => new DefaultValueAttribute(typeof(string)));
            Assert.Throws<ArgumentNullException>(() => new DefaultValueAttribute((Type)null!));
        }

        #endregion

        #region Private-Methods

        private static ResolverSample Sample(int id, string name, int count, params string[] tags)
        {
            return new ResolverSample { Id = id, Name = name, Count = count, Tags = new List<string>(tags) };
        }

        private static string TimestampBytes(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();
            return Convert.ToHexString(bytes, 10, 6);
        }

        #endregion
    }
}
