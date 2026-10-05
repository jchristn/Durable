namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Coverage for raw SQL mapping (DTOs, scalars, multiple result sets), stored procedures (input, output and
    /// result-set parameters), command timeouts and cancellation. Executed identically across all database
    /// providers; provider-specific SQL is chosen per <see cref="IRepositoryProvider.DatabaseType"/>.
    /// </summary>
    public class RawSqlAndProcedureTestSuite : IDisposable
    {
        #region Private-Members

        private const string Department = "RawSqlDept";
        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="RawSqlAndProcedureTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the configured database.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="provider"/> is null.</exception>
        public RawSqlAndProcedureTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// FromSql maps snake_case result columns onto PascalCase DTO properties.
        /// </summary>
        [Fact]
        public async Task FromSql_MapsSnakeCaseColumnsToDto()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<PersonNameDto> rows = repository.FromSql<PersonNameDto>(
                "SELECT first AS first_name, last AS last_name, age AS person_age FROM people WHERE department = @p0 ORDER BY age",
                null,
                Department).ToList();

            Assert.Equal(3, rows.Count);
            Assert.Equal("Ann", rows[0].FirstName);
            Assert.Equal("Alpha", rows[0].LastName);
            Assert.Equal(21, rows[0].PersonAge);
            Assert.Equal("Cid", rows[2].FirstName);
            Assert.Equal(23, rows[2].PersonAge);

            List<PersonNameDto> asyncRows = new List<PersonNameDto>();
            await foreach (PersonNameDto dto in repository.FromSqlAsync<PersonNameDto>(
                "SELECT first AS first_name, last AS last_name, age AS person_age FROM people WHERE department = @p0 AND age > @p1",
                null,
                default,
                Department,
                21))
            {
                asyncRows.Add(dto);
            }

            Assert.Equal(2, asyncRows.Count);
            Assert.All(asyncRows, r => Assert.False(string.IsNullOrEmpty(r.LastName)));
            await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
        }

        /// <summary>
        /// FromSql with a scalar result type reads the first column of each row.
        /// </summary>
        [Fact]
        public async Task FromSql_ScalarFirstColumn()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            List<int> ages = repository.FromSql<int>("SELECT age, first FROM people WHERE department = @p0 ORDER BY age", null, Department).ToList();
            Assert.Equal(new List<int> { 21, 22, 23 }, ages);

            List<long> counts = new List<long>();
            await foreach (long value in repository.FromSqlAsync<long>("SELECT COUNT(*) FROM people WHERE department = @p0", null, default, Department))
                counts.Add(value);
            Assert.Equal(new List<long> { 3 }, counts);

            List<decimal> salaries = repository.FromSql<decimal>("SELECT salary FROM people WHERE department = @p0 ORDER BY age", null, Department).ToList();
            Assert.Equal(3, salaries.Count);
            Assert.Equal(50000m, salaries[0]);
            await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
        }

        /// <summary>
        /// ExecuteScalar converts the first column of the first row, and returns default when there are no rows.
        /// </summary>
        [Fact]
        public async Task ExecuteScalar_ReturnsConvertedValue()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            long count = repository.ExecuteScalar<long>("SELECT COUNT(*) FROM people WHERE department = @p0", null, Department);
            Assert.Equal(3L, count);

            long asyncCount = await repository.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM people WHERE department = @p0", null, default, Department);
            Assert.Equal(3L, asyncCount);

            string? first = await repository.ExecuteScalarAsync<string>("SELECT first FROM people WHERE department = @p0 AND age = @p1", null, default, Department, 22);
            Assert.Equal("Bob", first);

            string? missing = repository.ExecuteScalar<string>("SELECT first FROM people WHERE department = @p0 AND age = @p1", null, Department, -1);
            Assert.Null(missing);
            await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
        }

        /// <summary>
        /// QueryMultiple reads two result sets of different shapes from one batch.
        /// </summary>
        [Fact]
        public async Task QueryMultiple_ReadsTwoResultSets()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            using (SqlMultipleResultReader reader = repository.QueryMultiple(
                "SELECT first AS first_name, last AS last_name, age AS person_age FROM people WHERE department = @p0 ORDER BY age; " +
                "SELECT COUNT(*) FROM people WHERE department = @p0",
                null,
                Department))
            {
                List<PersonNameDto> people = reader.Read<PersonNameDto>();
                Assert.True(reader.HasMoreResults);
                List<long> counts = reader.Read<long>();
                Assert.Equal(3, people.Count);
                Assert.Equal("Ann", people[0].FirstName);
                Assert.Equal(new List<long> { 3 }, counts);
                Assert.False(reader.HasMoreResults);
                Assert.Throws<InvalidOperationException>(() => reader.Read<long>());
            }

            await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
        }

        /// <summary>
        /// QueryMultipleAsync reads two result sets of different shapes from one batch.
        /// </summary>
        [Fact]
        public async Task QueryMultipleAsync_ReadsTwoResultSets()
        {
            ISqlRepository<Person> repository = await SeedAsync();

            SqlMultipleResultReader reader = await repository.QueryMultipleAsync(
                "SELECT age FROM people WHERE department = @p0 ORDER BY age; " +
                "SELECT first AS first_name, last AS last_name, age AS person_age FROM people WHERE department = @p0 AND age >= @p1 ORDER BY age",
                null,
                default,
                Department,
                22);
            await using (reader)
            {
                List<int> ages = await reader.ReadAsync<int>();
                List<PersonNameDto> older = await reader.ReadAsync<PersonNameDto>();
                Assert.Equal(new List<int> { 21, 22, 23 }, ages);
                Assert.Equal(2, older.Count);
                Assert.Equal("Bob", older[0].FirstName);
                Assert.Equal(23, older[1].PersonAge);
            }

            await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
        }

        /// <summary>
        /// On SQLite, stored procedure calls throw <see cref="NotSupportedException"/>.
        /// </summary>
        [Fact]
        public async Task StoredProcedure_SqliteNotSupported()
        {
            if (_Provider.DatabaseType != TestDatabaseType.Sqlite) return;

            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            Assert.Throws<NotSupportedException>(() => repository.ExecuteProcedure("anything"));
            Assert.Throws<NotSupportedException>(() => repository.FromProcedure<PersonNameDto>("anything"));
            await Assert.ThrowsAsync<NotSupportedException>(() => repository.ExecuteProcedureAsync("anything"));
            await Assert.ThrowsAsync<NotSupportedException>(() => repository.FromProcedureAsync<PersonNameDto>("anything"));
        }

        /// <summary>
        /// A stored procedure with an input parameter returns a result set that maps onto a DTO
        /// (MySQL and SQL Server). On PostgreSQL a procedure with an INOUT parameter returns its single row.
        /// </summary>
        [Fact]
        public async Task StoredProcedure_InputParameterReturnsResultSet()
        {
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite) return;

            ISqlRepository<Person> repository = await SeedAsync();
            try
            {
                switch (_Provider.DatabaseType)
                {
                    case TestDatabaseType.SqlServer:
                        await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_people_by_dept");
                        await repository.ExecuteSqlAsync(
                            "CREATE PROCEDURE durable_people_by_dept @dept NVARCHAR(32) AS " +
                            "SELECT first AS first_name, last AS last_name, age AS person_age FROM people WHERE department = @dept ORDER BY age");
                        await AssertProcedureRowsAsync(repository, new SqlParameterValue("@dept", Department));
                        break;

                    case TestDatabaseType.MySql:
                        await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_people_by_dept");
                        await repository.ExecuteSqlAsync(
                            "CREATE PROCEDURE durable_people_by_dept(IN dept VARCHAR(32)) BEGIN " +
                            "SELECT first AS first_name, last AS last_name, age AS person_age FROM people WHERE department = dept ORDER BY age; END");
                        await AssertProcedureRowsAsync(repository, new SqlParameterValue("@dept", Department));
                        break;

                    case TestDatabaseType.Postgres:
                        await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_count_by_dept");
                        await repository.ExecuteSqlAsync(
                            "CREATE PROCEDURE durable_count_by_dept(IN dept VARCHAR, INOUT total BIGINT) LANGUAGE plpgsql AS $$ " +
                            "BEGIN SELECT COUNT(*) INTO total FROM people WHERE department = dept; END $$");
                        List<long> rows = await repository.FromProcedureAsync<long>(
                            "durable_count_by_dept",
                            null,
                            default,
                            new SqlParameterValue("dept", Department),
                            new SqlParameterValue("total", null) { Direction = ParameterDirection.InputOutput, DbType = DbType.Int64 });
                        Assert.Equal(new List<long> { 3 }, rows);
                        break;
                }
            }
            finally
            {
                await DropProceduresAsync(repository);
                await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
            }
        }

        /// <summary>
        /// An OUTPUT (SQL Server, MySQL) or INOUT (PostgreSQL) parameter declared with
        /// <see cref="SqlParameterValue.Direction"/> receives the procedure's value after ExecuteProcedure.
        /// </summary>
        [Fact]
        public async Task StoredProcedure_OutputParameterIsPopulated()
        {
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite) return;

            ISqlRepository<Person> repository = await SeedAsync();
            try
            {
                SqlParameterValue output;
                switch (_Provider.DatabaseType)
                {
                    case TestDatabaseType.SqlServer:
                        await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_count_by_dept");
                        await repository.ExecuteSqlAsync(
                            "CREATE PROCEDURE durable_count_by_dept @dept NVARCHAR(32), @total INT OUTPUT AS " +
                            "SELECT @total = COUNT(*) FROM people WHERE department = @dept");
                        output = new SqlParameterValue("@total", null) { Direction = ParameterDirection.Output, DbType = DbType.Int32 };
                        repository.ExecuteProcedure("durable_count_by_dept", null, new SqlParameterValue("@dept", Department), output);
                        Assert.Equal(3L, Convert.ToInt64(output.Value));
                        break;

                    case TestDatabaseType.MySql:
                        await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_count_by_dept");
                        await repository.ExecuteSqlAsync(
                            "CREATE PROCEDURE durable_count_by_dept(IN dept VARCHAR(32), OUT total INT) BEGIN " +
                            "SELECT COUNT(*) INTO total FROM people WHERE department = dept; END");
                        output = new SqlParameterValue("@total", null) { Direction = ParameterDirection.Output, DbType = DbType.Int32 };
                        await repository.ExecuteProcedureAsync("durable_count_by_dept", null, default, new SqlParameterValue("@dept", Department), output);
                        Assert.Equal(3L, Convert.ToInt64(output.Value));
                        break;

                    case TestDatabaseType.Postgres:
                        await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_count_by_dept");
                        await repository.ExecuteSqlAsync(
                            "CREATE PROCEDURE durable_count_by_dept(IN dept VARCHAR, INOUT total BIGINT) LANGUAGE plpgsql AS $$ " +
                            "BEGIN SELECT COUNT(*) INTO total FROM people WHERE department = dept; END $$");
                        output = new SqlParameterValue("total", null) { Direction = ParameterDirection.InputOutput, DbType = DbType.Int64 };
                        await repository.ExecuteProcedureAsync("durable_count_by_dept", null, default, new SqlParameterValue("dept", Department), output);
                        Assert.Equal(3L, Convert.ToInt64(output.Value));
                        break;
                }
            }
            finally
            {
                await DropProceduresAsync(repository);
                await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
            }
        }

        /// <summary>
        /// With CommandTimeoutSeconds = 1, a statement that sleeps for 3 seconds fails well before it would finish.
        /// Skipped on SQLite, which has no server-side sleep.
        /// </summary>
        [Fact]
        public async Task CommandTimeout_SlowStatementFails()
        {
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite) return;

            SqlRepositoryOptions options = new SqlRepositoryOptions { CommandTimeoutSeconds = 1 };
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);

            Stopwatch stopwatch = Stopwatch.StartNew();
            Exception thrown = await Assert.ThrowsAnyAsync<Exception>(() => repository.ExecuteSqlAsync(SleepSql(3)));
            stopwatch.Stop();

            Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(2900), "Timed out after " + stopwatch.ElapsedMilliseconds + "ms; expected ~1s. Exception: " + thrown.GetType().Name + ": " + thrown.Message);
            Assert.True(thrown is DbException || thrown is TimeoutException || thrown is OperationCanceledException || thrown.InnerException is TimeoutException,
                "Unexpected exception type " + thrown.GetType().FullName + ": " + thrown.Message);

            Assert.True(await repository.CountAsync() >= 0);
        }

        /// <summary>
        /// A token canceled before the call makes ReadManyAsync, CountAsync and ExecuteScalarAsync throw
        /// <see cref="OperationCanceledException"/>.
        /// </summary>
        [Fact]
        public async Task Cancellation_PreCanceledTokenThrows()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            using CancellationTokenSource source = new CancellationTokenSource();
            source.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (Person person in repository.ReadManyAsync(null, null, source.Token))
                {
                }
            });
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.CountAsync(null, null, source.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM people", null, source.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => repository.CreateAsync(InfrastructureTestData.NewPerson("cancel@example.com", "CancelDept"), null, source.Token));
            Assert.Equal(0, await repository.CountAsync(p => p.Email == "cancel@example.com"));
        }

        /// <summary>
        /// Canceling a token while a slow statement executes aborts it promptly with an
        /// <see cref="OperationCanceledException"/> or a provider exception reporting the cancellation.
        /// Skipped on SQLite, which has no server-side sleep.
        /// </summary>
        [Fact]
        public async Task Cancellation_DuringExecutionAborts()
        {
            if (_Provider.DatabaseType == TestDatabaseType.Sqlite) return;

            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            using CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

            Stopwatch stopwatch = Stopwatch.StartNew();
            Exception thrown = await Assert.ThrowsAnyAsync<Exception>(() => repository.ExecuteSqlAsync(SleepSql(5), null, source.Token));
            stopwatch.Stop();

            Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(4500), "Cancellation took " + stopwatch.ElapsedMilliseconds + "ms");
            bool reportsCancellation = thrown is OperationCanceledException
                || thrown.InnerException is OperationCanceledException
                || thrown.Message.Contains("cancel", StringComparison.OrdinalIgnoreCase)
                || thrown.Message.Contains("interrupted", StringComparison.OrdinalIgnoreCase);
            Assert.True(reportsCancellation, "Unexpected exception " + thrown.GetType().FullName + ": " + thrown.Message);

            Assert.True(await repository.CountAsync() >= 0);
        }

        /// <summary>
        /// Canceling while streaming ReadManyAsync results stops enumeration with an <see cref="OperationCanceledException"/>.
        /// </summary>
        [Fact]
        public async Task Cancellation_DuringEnumerationStops()
        {
            ISqlRepository<Person> repository = await SeedAsync();
            using CancellationTokenSource source = new CancellationTokenSource();
            int seen = 0;

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (Person person in repository.ReadManyAsync(p => p.Department == Department, null, source.Token))
                {
                    seen++;
                    source.Cancel();
                }
            });

            Assert.Equal(1, seen);
            await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<Person>> SeedAsync()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, Department);
            string[] firsts = new[] { "Ann", "Bob", "Cid" };
            string[] lasts = new[] { "Alpha", "Bravo", "Charlie" };
            for (int i = 0; i < 3; i++)
            {
                Person person = InfrastructureTestData.NewPerson("raw-" + i + "@example.com", Department, 21 + i);
                person.FirstName = firsts[i];
                person.LastName = lasts[i];
                await repository.CreateAsync(person);
            }

            return repository;
        }

        private static async Task AssertProcedureRowsAsync(ISqlRepository<Person> repository, SqlParameterValue parameter)
        {
            List<PersonNameDto> rows = repository.FromProcedure<PersonNameDto>("durable_people_by_dept", null, parameter);
            Assert.Equal(3, rows.Count);
            Assert.Equal("Ann", rows[0].FirstName);
            Assert.Equal("Charlie", rows[2].LastName);
            Assert.Equal(23, rows[2].PersonAge);

            List<PersonNameDto> asyncRows = await repository.FromProcedureAsync<PersonNameDto>("durable_people_by_dept", null, default, parameter);
            Assert.Equal(3, asyncRows.Count);
        }

        private async Task DropProceduresAsync(ISqlRepository<Person> repository)
        {
            try
            {
                await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_people_by_dept");
                await repository.ExecuteSqlAsync("DROP PROCEDURE IF EXISTS durable_count_by_dept");
            }
            catch (DbException)
            {
            }
        }

        private string SleepSql(int seconds)
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Postgres: return "SELECT pg_sleep(" + seconds + ")";
                // SLEEP() returns 1 instead of raising an error when MySQL kills it (as MySqlConnector does on
                // timeout/cancel), so use a long-running cross join that fails with ER_QUERY_INTERRUPTED instead.
                case TestDatabaseType.MySql: return "SELECT COUNT(*) FROM information_schema.columns a CROSS JOIN information_schema.columns b CROSS JOIN information_schema.columns c";
                case TestDatabaseType.SqlServer: return "WAITFOR DELAY '00:00:0" + seconds + "'";
                default: throw new NotSupportedException("No sleep statement for " + _Provider.DatabaseType);
            }
        }

        #endregion
    }
}
