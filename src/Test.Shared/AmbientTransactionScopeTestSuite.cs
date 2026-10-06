namespace Test.Shared
{
    using System;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Durable.Sqlite;
    using Xunit;

    /// <summary>
    /// Coverage for ambient <see cref="AmbientTransactionScope"/> behavior: <see cref="AmbientTransactionScope.CreateAsync{T}"/>
    /// publishes the scope to the caller, repository calls without an explicit transaction join it, disposal
    /// without completion rolls back, completion commits, nested scopes share the transaction, repositories of
    /// different entity types share one scope, and the <c>ExecuteInTransactionScopeAsync</c> extension commits or
    /// rolls back. Executed identically across all database providers.
    /// </summary>
    public class AmbientTransactionScopeTestSuite : IDisposable
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="AmbientTransactionScopeTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the configured database.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="provider"/> is null.</exception>
        public AmbientTransactionScopeTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// After awaiting CreateAsync the scope is visible as <see cref="AmbientTransactionScope.Current"/> in the caller,
        /// and is cleared again once disposed.
        /// </summary>
        [Fact]
        public async Task CreateAsync_SetsCurrentInCaller()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            Assert.Null(AmbientTransactionScope.Current);

            using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(repository))
            {
                Assert.NotNull(AmbientTransactionScope.Current);
                Assert.Same(scope, AmbientTransactionScope.Current);
                Assert.IsAssignableFrom<ISqlTransaction>(scope.Transaction);
            }

            Assert.Null(AmbientTransactionScope.Current);
        }

        /// <summary>
        /// Writes without an explicit transaction join the ambient scope, are visible inside it, and are rolled
        /// back when the scope is disposed without completion.
        /// </summary>
        [Fact]
        public async Task AmbientScope_JoinsAndRollsBackWithoutComplete()
        {
            const string department = "TxScopeRollback";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(repository))
            {
                await repository.CreateAsync(InfrastructureTestData.NewPerson("scope-rb-1@example.com", department));
                repository.Create(InfrastructureTestData.NewPerson("scope-rb-2@example.com", department));

                long inside = await repository.CountAsync(p => p.Department == department);
                Assert.Equal(2, inside);

                long insideViaExplicit = await repository.CountAsync(p => p.Department == department, scope.Transaction);
                Assert.Equal(2, insideViaExplicit);
            }

            long after = await repository.CountAsync(p => p.Department == department);
            Assert.Equal(0, after);
        }

        /// <summary>
        /// <c>await using</c> disposes the scope asynchronously: an uncompleted scope rolls back with RollbackAsync and
        /// stops being current in the caller, and a second DisposeAsync is a no-op.
        /// </summary>
        [Fact]
        public async Task AwaitUsing_DisposeAsyncRollsBackAndClearsCurrent()
        {
            const string department = "TxScopeAsyncDispose";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            AmbientTransactionScope captured;
            await using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(repository))
            {
                captured = scope;
                await repository.CreateAsync(InfrastructureTestData.NewPerson("scope-ad-1@example.com", department));
                Assert.Equal(1, await repository.CountAsync(p => p.Department == department));
                Assert.Same(scope, AmbientTransactionScope.Current);
            }

            Assert.Null(AmbientTransactionScope.Current);
            Assert.True(captured.Transaction.IsCompleted);
            await captured.DisposeAsync();
            Assert.Equal(0, await repository.CountAsync(p => p.Department == department));
        }

        /// <summary>
        /// The synchronous ExecuteInTransactionScope overloads (repository and transaction forms) commit when the
        /// delegate returns and roll back when it throws.
        /// </summary>
        [Fact]
        public async Task ExecuteInTransactionScope_SyncOverloadsCommitAndRollBack()
        {
            const string department = "TxScopeSyncExec";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            repository.ExecuteInTransactionScope(() => { repository.Create(InfrastructureTestData.NewPerson("scope-se-1@example.com", department)); });
            int returned = repository.ExecuteInTransactionScope(() => { repository.Create(InfrastructureTestData.NewPerson("scope-se-2@example.com", department)); return 7; });
            Assert.Equal(7, returned);
            Assert.Throws<InvalidOperationException>(() => repository.ExecuteInTransactionScope(() =>
            {
                repository.Create(InfrastructureTestData.NewPerson("scope-se-3@example.com", department));
                throw new InvalidOperationException("boom");
            }));
            Assert.Equal(2, await repository.CountAsync(p => p.Department == department));

            using (ITransaction transaction = repository.BeginTransaction())
            {
                transaction.ExecuteInTransactionScope(() => { repository.Create(InfrastructureTestData.NewPerson("scope-se-4@example.com", department)); });
                Assert.True(transaction.IsCompleted);
            }

            using (ITransaction transaction = repository.BeginTransaction())
            {
                Assert.Throws<InvalidOperationException>(() => transaction.ExecuteInTransactionScope<int>(() =>
                {
                    repository.Create(InfrastructureTestData.NewPerson("scope-se-5@example.com", department));
                    throw new InvalidOperationException("boom");
                }));
            }

            Assert.Equal(3, await repository.CountAsync(p => p.Department == department));
            Assert.Null(AmbientTransactionScope.Current);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// Writes inside an ambient scope persist after CompleteAsync.
        /// </summary>
        [Fact]
        public async Task AmbientScope_CompleteAsyncPersists()
        {
            const string department = "TxScopeCommit";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(repository))
            {
                await repository.CreateAsync(InfrastructureTestData.NewPerson("scope-c-1@example.com", department));
                await repository.CreateAsync(InfrastructureTestData.NewPerson("scope-c-2@example.com", department));
                await scope.CompleteAsync();
                Assert.True(scope.IsCompleted);
            }

            long after = await repository.CountAsync(p => p.Department == department);
            Assert.Equal(2, after);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// Updates and deletes without an explicit transaction also join the ambient scope and are undone on rollback.
        /// </summary>
        [Fact]
        public async Task AmbientScope_UpdateAndDeleteAreRolledBack()
        {
            const string department = "TxScopeUpdDel";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
            Person keep = await repository.CreateAsync(InfrastructureTestData.NewPerson("scope-ud-1@example.com", department, 40));
            Person remove = await repository.CreateAsync(InfrastructureTestData.NewPerson("scope-ud-2@example.com", department, 41));

            using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(repository))
            {
                keep.Age = 99;
                await repository.UpdateAsync(keep);
                await repository.DeleteAsync(remove);
                Assert.Equal(1, await repository.CountAsync(p => p.Department == department));
                Person? updatedInside = await repository.ReadByIdAsync(keep.Id);
                Assert.NotNull(updatedInside);
                Assert.Equal(99, updatedInside!.Age);
            }

            Assert.Equal(2, await repository.CountAsync(p => p.Department == department));
            Person? reread = await repository.ReadByIdAsync(keep.Id);
            Assert.NotNull(reread);
            Assert.Equal(40, reread!.Age);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// A nested scope created over the outer scope's transaction shares it: completing the nested scope does
        /// not commit, and disposing the outer scope without completion rolls back everything. Passing the scope's
        /// transaction explicitly also targets the same transaction.
        /// </summary>
        [Fact]
        public async Task NestedScope_SharesTransactionAndOuterRollbackWins()
        {
            const string department = "TxScopeNested";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (AmbientTransactionScope outer = await AmbientTransactionScope.CreateAsync(repository))
            {
                await repository.CreateAsync(InfrastructureTestData.NewPerson("nested-outer@example.com", department), outer.Transaction);

                using (AmbientTransactionScope inner = AmbientTransactionScope.Create(outer.Transaction))
                {
                    Assert.Same(inner, AmbientTransactionScope.Current);
                    Assert.Same(outer.Transaction, inner.Transaction);
                    await repository.CreateAsync(InfrastructureTestData.NewPerson("nested-inner@example.com", department));
                    await inner.CompleteAsync();
                }

                Assert.Same(outer, AmbientTransactionScope.Current);
                Assert.False(outer.Transaction.IsCompleted);
                Assert.Equal(2, await repository.CountAsync(p => p.Department == department));
            }

            Assert.Equal(0, await repository.CountAsync(p => p.Department == department));
        }

        /// <summary>
        /// A nested scope that shares the outer transaction, followed by outer completion, persists both writes.
        /// </summary>
        [Fact]
        public async Task NestedScope_OuterCompletePersistsBoth()
        {
            const string department = "TxScopeNestedOk";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (AmbientTransactionScope outer = await AmbientTransactionScope.CreateAsync(repository))
            {
                await repository.CreateAsync(InfrastructureTestData.NewPerson("nested-ok-outer@example.com", department));
                using (AmbientTransactionScope inner = AmbientTransactionScope.Create(outer.Transaction))
                {
                    await repository.CreateAsync(InfrastructureTestData.NewPerson("nested-ok-inner@example.com", department));
                    inner.Complete();
                }

                await outer.CompleteAsync();
            }

            Assert.Equal(2, await repository.CountAsync(p => p.Department == department));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// Repositories for two different entity types (each with its own connection factory) both join the same
        /// ambient scope and roll back together.
        /// </summary>
        [Fact]
        public async Task TwoEntityTypes_ShareScopeAndRollBackTogether()
        {
            const string department = "TxScopeTwoTypes";
            const string categoryName = "TxScopeTwoTypesCategory";
            ISqlRepository<Person> people = _Provider.CreateRepository<Person>();
            ISqlRepository<Category> categories = _Provider.CreateRepository<Category>();
            await InfrastructureTestData.ClearDepartmentAsync(people, department);
            await categories.DeleteManyAsync(c => c.Name == categoryName);

            using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(people))
            {
                await people.CreateAsync(InfrastructureTestData.NewPerson("two-types@example.com", department));
                await categories.CreateAsync(new Category { Name = categoryName, Description = "scope" });
                Assert.Equal(1, await categories.CountAsync(c => c.Name == categoryName));
                Assert.Equal(1, await people.CountAsync(p => p.Department == department));
            }

            Assert.Equal(0, await people.CountAsync(p => p.Department == department));
            Assert.Equal(0, await categories.CountAsync(c => c.Name == categoryName));
        }

        /// <summary>
        /// Repositories for two different entity types commit together when the scope is completed.
        /// </summary>
        [Fact]
        public async Task TwoEntityTypes_CommitTogether()
        {
            const string department = "TxScopeTwoTypesOk";
            const string categoryName = "TxScopeTwoTypesOkCategory";
            ISqlRepository<Person> people = _Provider.CreateRepository<Person>();
            ISqlRepository<Category> categories = _Provider.CreateRepository<Category>();
            await InfrastructureTestData.ClearDepartmentAsync(people, department);
            await categories.DeleteManyAsync(c => c.Name == categoryName);

            using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(categories))
            {
                await people.CreateAsync(InfrastructureTestData.NewPerson("two-types-ok@example.com", department));
                await categories.CreateAsync(new Category { Name = categoryName, Description = "scope" });
                await scope.CompleteAsync();
            }

            Assert.Equal(1, await people.CountAsync(p => p.Department == department));
            Assert.Equal(1, await categories.CountAsync(c => c.Name == categoryName));
            await InfrastructureTestData.ClearDepartmentAsync(people, department);
            await categories.DeleteManyAsync(c => c.Name == categoryName);
        }

        /// <summary>
        /// ExecuteInTransactionScopeAsync commits when the delegate succeeds.
        /// </summary>
        [Fact]
        public async Task ExecuteInTransactionScopeAsync_CommitsOnSuccess()
        {
            const string department = "TxScopeExecOk";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            await repository.ExecuteInTransactionScopeAsync(async () =>
            {
                Assert.NotNull(AmbientTransactionScope.Current);
                await repository.CreateAsync(InfrastructureTestData.NewPerson("exec-ok-1@example.com", department));
                await repository.CreateAsync(InfrastructureTestData.NewPerson("exec-ok-2@example.com", department));
            });

            Assert.Null(AmbientTransactionScope.Current);
            Assert.Equal(2, await repository.CountAsync(p => p.Department == department));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// ExecuteInTransactionScopeAsync rolls back and rethrows when the delegate throws.
        /// </summary>
        [Fact]
        public async Task ExecuteInTransactionScopeAsync_RollsBackOnException()
        {
            const string department = "TxScopeExecFail";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            InvalidOperationException thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                repository.ExecuteInTransactionScopeAsync(async () =>
                {
                    await repository.CreateAsync(InfrastructureTestData.NewPerson("exec-fail@example.com", department));
                    throw new InvalidOperationException("boom");
                }));

            Assert.Equal("boom", thrown.Message);
            Assert.Null(AmbientTransactionScope.Current);
            Assert.Equal(0, await repository.CountAsync(p => p.Department == department));
        }

        /// <summary>
        /// The result-returning ExecuteInTransactionScopeAsync overload returns the delegate's value and commits.
        /// </summary>
        [Fact]
        public async Task ExecuteInTransactionScopeAsync_ReturnsResult()
        {
            const string department = "TxScopeExecResult";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            Person created = await repository.ExecuteInTransactionScopeAsync(async () =>
                await repository.CreateAsync(InfrastructureTestData.NewPerson("exec-result@example.com", department)));

            Assert.True(created.Id > 0);
            Assert.True(await repository.ExistsByIdAsync(created.Id));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// An ambient scope owned by a different provider is ignored: writes go to their own connection and
        /// persist even though the foreign scope is rolled back. On SQLite a PostgreSQL-typed wrapper is not
        /// creatable as an ambient scope without a server, so a private in-memory SQLite scope is used on the
        /// other providers only.
        /// </summary>
        [Fact]
        public async Task ForeignProviderAmbientScope_IsIgnored()
        {
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite) return;

            const string department = "TxScopeForeign";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            using (SqliteRepository<Person> foreign = new SqliteRepository<Person>("Data Source=:memory:"))
            {
                using (AmbientTransactionScope scope = await AmbientTransactionScope.CreateAsync(foreign))
                {
                    await repository.CreateAsync(InfrastructureTestData.NewPerson("foreign-scope@example.com", department));
                }
            }

            Assert.Equal(1, await repository.CountAsync(p => p.Department == department));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
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
