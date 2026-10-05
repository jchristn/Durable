namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Sql;
    using Microsoft.Extensions.Logging;
    using Xunit;

    /// <summary>
    /// Coverage for command interceptors, OpenTelemetry-style tracing through <see cref="DurableDiagnostics"/>,
    /// <see cref="ILogger"/> logging, SQL capture and the parameterization guarantee for predicate values.
    /// Executed identically across all database providers.
    /// </summary>
    public class DiagnosticsTestSuite : IDisposable
    {
        #region Private-Members

        private const string InjectionText = "'; DROP TABLE people; --";
        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="DiagnosticsTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The repository provider for the configured database.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="provider"/> is null.</exception>
        public DiagnosticsTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// An interceptor sees Executing and Executed for INSERT, SELECT, UPDATE, DELETE and RAW operations.
        /// </summary>
        [Fact]
        public async Task Interceptor_SeesOperationNames()
        {
            const string department = "DiagInterceptor";
            RecordingInterceptor interceptor = new RecordingInterceptor();
            SqlRepositoryOptions options = new SqlRepositoryOptions();
            options.Interceptors.Add(interceptor);
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            Person created = await repository.CreateAsync(InfrastructureTestData.NewPerson("interceptor@example.com", department));
            List<Person> read = new List<Person>();
            await foreach (Person person in repository.ReadManyAsync(p => p.Department == department)) read.Add(person);
            Assert.Single(read);
            created.Age = 31;
            await repository.UpdateAsync(created);
            await repository.ExecuteSqlAsync("UPDATE people SET age = 32 WHERE department = @p0", null, default, department);
            await repository.DeleteAsync(created);

            List<string> events = interceptor.Events();
            foreach (string operation in new[] { "INSERT", "SELECT", "UPDATE", "RAW", "DELETE" })
            {
                Assert.Contains("Executing:" + operation, events);
                Assert.Contains("Executed:" + operation, events);
            }

            Assert.DoesNotContain(events, e => e.StartsWith("Failed:", StringComparison.Ordinal));
            int executing = events.Count(e => e.StartsWith("Executing:", StringComparison.Ordinal));
            int executed = events.Count(e => e.StartsWith("Executed:", StringComparison.Ordinal));
            Assert.Equal(executing, executed);
        }

        /// <summary>
        /// An interceptor sees Failed (and no Executed) for a statement the database rejects.
        /// </summary>
        [Fact]
        public async Task Interceptor_SeesFailedOnBadSql()
        {
            RecordingInterceptor interceptor = new RecordingInterceptor();
            SqlRepositoryOptions options = new SqlRepositoryOptions();
            options.Interceptors.Add(interceptor);
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);

            await Assert.ThrowsAnyAsync<Exception>(() => repository.ExecuteSqlAsync("SELECT * FROM durable_no_such_table_xyz"));

            List<string> events = interceptor.Events();
            Assert.Equal(new List<string> { "Executing:RAW", "Failed:RAW" }, events);
            Assert.NotNull(interceptor.LastFailure);
        }

        /// <summary>
        /// An interceptor can rewrite command text (append a comment); the command still runs and returns correct results.
        /// </summary>
        [Fact]
        public async Task Interceptor_CanModifyCommandText()
        {
            const string department = "DiagRewrite";
            RecordingInterceptor interceptor = new RecordingInterceptor { AppendComment = "/* durable-intercepted */" };
            SqlRepositoryOptions options = new SqlRepositoryOptions();
            options.Interceptors.Add(interceptor);
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            Person created = await repository.CreateAsync(InfrastructureTestData.NewPerson("rewrite@example.com", department));
            Assert.True(created.Id > 0);
            Assert.Equal(1, await repository.CountAsync(p => p.Department == department));
            Person? reread = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(reread);
            Assert.Equal("rewrite@example.com", reread!.Email);

            List<string> texts = interceptor.ExecutedCommandText();
            Assert.NotEmpty(texts);
            Assert.All(texts, t => Assert.EndsWith("/* durable-intercepted */", t));
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// An <see cref="ActivityListener"/> on the Durable source receives client activities tagged with
        /// db.system, db.collection.name, db.operation.name and db.query.text.
        /// </summary>
        [Fact]
        public async Task Tracing_EmitsActivitiesWithTags()
        {
            const string department = "DiagTracing";
            List<Activity> stopped = new List<Activity>();
            object sync = new object();
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == DurableDiagnostics.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => { lock (sync) stopped.Add(activity); }
            };
            ActivitySource.AddActivityListener(listener);

            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
            await repository.CreateAsync(InfrastructureTestData.NewPerson("tracing@example.com", department));
            List<Person> read = new List<Person>();
            await foreach (Person person in repository.ReadManyAsync(p => p.Department == department)) read.Add(person);
            Assert.Single(read);

            List<Activity> snapshot;
            lock (sync) snapshot = new List<Activity>(stopped);

            Activity? select = snapshot.LastOrDefault(a => (string?)a.GetTagItem("db.operation.name") == "SELECT" && ((string?)a.GetTagItem("db.query.text") ?? string.Empty).Contains("department", StringComparison.OrdinalIgnoreCase));
            Assert.NotNull(select);
            Assert.Equal(ActivityKind.Client, select!.Kind);
            Assert.Equal(ExpectedDbSystem(), (string?)select.GetTagItem("db.system"));
            Assert.Equal("people", (string?)select.GetTagItem("db.collection.name"));
            Assert.False(string.IsNullOrWhiteSpace((string?)select.GetTagItem("db.query.text")));
            Assert.NotEqual(ActivityStatusCode.Error, select.Status);

            Activity? insert = snapshot.LastOrDefault(a => (string?)a.GetTagItem("db.operation.name") == "INSERT");
            Assert.NotNull(insert);
            Assert.Equal("people", (string?)insert!.GetTagItem("db.collection.name"));
            Assert.Contains("people", (string?)insert.GetTagItem("db.query.text") ?? string.Empty, StringComparison.OrdinalIgnoreCase);

            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// A failed command produces an activity with <see cref="ActivityStatusCode.Error"/>.
        /// </summary>
        [Fact]
        public async Task Tracing_FailedCommandSetsErrorStatus()
        {
            List<Activity> stopped = new List<Activity>();
            object sync = new object();
            using ActivityListener listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == DurableDiagnostics.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => { lock (sync) stopped.Add(activity); }
            };
            ActivitySource.AddActivityListener(listener);

            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await Assert.ThrowsAnyAsync<Exception>(() => repository.ExecuteSqlAsync("SELECT * FROM durable_trace_missing_table"));

            List<Activity> snapshot;
            lock (sync) snapshot = new List<Activity>(stopped);
            Activity? failed = snapshot.LastOrDefault(a => ((string?)a.GetTagItem("db.query.text") ?? string.Empty).Contains("durable_trace_missing_table", StringComparison.Ordinal));
            Assert.NotNull(failed);
            Assert.Equal(ActivityStatusCode.Error, failed!.Status);
            Assert.Equal("RAW", (string?)failed.GetTagItem("db.operation.name"));
            Assert.Equal(ExpectedDbSystem(), (string?)failed.GetTagItem("db.system"));
        }

        /// <summary>
        /// With no slow threshold, successful commands are logged at Debug; parameter values are omitted by default.
        /// </summary>
        [Fact]
        public async Task Logging_DebugForCommandsWithoutParameterValues()
        {
            const string department = "DiagLogDebug";
            const string secret = "log-secret-value@example.com";
            InMemoryLogger logger = new InMemoryLogger();
            SqlRepositoryOptions options = new SqlRepositoryOptions { Logger = logger, SlowCommandThreshold = null };
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
            logger.Clear();

            await repository.CountAsync(p => p.Email == secret);

            List<LogEntry> entries = logger.Snapshot();
            Assert.NotEmpty(entries);
            Assert.Contains(entries, e => e.Level == LogLevel.Debug && e.Message.Contains("people", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(entries, e => e.Level >= LogLevel.Warning);
            Assert.DoesNotContain(entries, e => e.Message.Contains(secret, StringComparison.Ordinal));
        }

        /// <summary>
        /// With LogParameterValues = true the logged SQL includes the parameter value.
        /// </summary>
        [Fact]
        public async Task Logging_ParameterValuesWhenEnabled()
        {
            const string secret = "log-visible-value@example.com";
            InMemoryLogger logger = new InMemoryLogger();
            SqlRepositoryOptions options = new SqlRepositoryOptions { Logger = logger, SlowCommandThreshold = null, LogParameterValues = true };
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);

            await repository.CountAsync(p => p.Email == secret);

            List<LogEntry> entries = logger.Snapshot();
            Assert.Contains(entries, e => e.Level == LogLevel.Debug && e.Message.Contains(secret, StringComparison.Ordinal));
        }

        /// <summary>
        /// With SlowCommandThreshold = zero every command is logged as a Warning.
        /// </summary>
        [Fact]
        public async Task Logging_WarningForSlowCommands()
        {
            InMemoryLogger logger = new InMemoryLogger();
            SqlRepositoryOptions options = new SqlRepositoryOptions { Logger = logger, SlowCommandThreshold = TimeSpan.Zero };
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);

            await repository.CountAsync();

            List<LogEntry> entries = logger.Snapshot();
            Assert.Contains(entries, e => e.Level == LogLevel.Warning && e.Message.Contains("Slow", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// A failed command is logged at Error with the exception attached.
        /// </summary>
        [Fact]
        public async Task Logging_ErrorOnFailure()
        {
            InMemoryLogger logger = new InMemoryLogger();
            SqlRepositoryOptions options = new SqlRepositoryOptions { Logger = logger };
            using ISqlRepository<Person> repository = _Provider.CreateRepositoryWithOptions<Person>(options);

            await Assert.ThrowsAnyAsync<Exception>(() => repository.ExecuteSqlAsync("SELECT * FROM durable_log_missing_table"));

            List<LogEntry> entries = logger.Snapshot();
            LogEntry? error = entries.LastOrDefault(e => e.Level == LogLevel.Error);
            Assert.NotNull(error);
            Assert.NotNull(error!.Exception);
            Assert.Contains("durable_log_missing_table", error.Message, StringComparison.Ordinal);
        }

        /// <summary>
        /// With CaptureSql enabled, LastExecutedSql contains placeholders rather than the predicate's literal value,
        /// while LastExecutedSqlWithParameters contains the value.
        /// </summary>
        [Fact]
        public async Task Capture_LastExecutedSqlIsParameterized()
        {
            const string literal = "capture-literal-value@example.com";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            repository.CaptureSql = true;

            List<Person> results = new List<Person>();
            await foreach (Person person in repository.ReadManyAsync(p => p.Email == "capture-literal-value@example.com")) results.Add(person);

            string? sql = repository.LastExecutedSql;
            string? withParameters = repository.LastExecutedSqlWithParameters;
            Assert.False(string.IsNullOrWhiteSpace(sql));
            Assert.DoesNotContain(literal, sql!, StringComparison.Ordinal);
            Assert.Contains("@", sql!, StringComparison.Ordinal);
            Assert.NotNull(withParameters);
            Assert.Contains(literal, withParameters!, StringComparison.Ordinal);
        }

        /// <summary>
        /// Captured SQL is also parameterized for a captured local variable and for an integer literal.
        /// </summary>
        [Fact]
        public void Capture_ParameterizesVariablesAndNumbers()
        {
            string variable = "capture-variable@example.com";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            repository.CaptureSql = true;

            repository.Count(p => p.Email == variable && p.Age == 12345);

            string? sql = repository.LastExecutedSql;
            Assert.NotNull(sql);
            Assert.DoesNotContain(variable, sql!, StringComparison.Ordinal);
            Assert.DoesNotContain("12345", sql!, StringComparison.Ordinal);
            Assert.Contains(variable, repository.LastExecutedSqlWithParameters!, StringComparison.Ordinal);
            Assert.Contains("12345", repository.LastExecutedSqlWithParameters!, StringComparison.Ordinal);
        }

        /// <summary>
        /// With CaptureSql disabled nothing is captured on the repository.
        /// </summary>
        [Fact]
        public async Task Capture_DisabledCapturesNothing()
        {
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            Assert.False(repository.CaptureSql);
            await repository.CountAsync();
            Assert.Null(repository.LastExecutedSql);
        }

        /// <summary>
        /// <see cref="SqlCaptureScope.Begin"/> captures statements across awaited async calls without enabling
        /// CaptureSql on the repository, and stops capturing once disposed.
        /// </summary>
        [Fact]
        public async Task CaptureScope_CapturesAcrossAwaits()
        {
            const string department = "DiagCaptureScope";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            Assert.False(repository.CaptureSql);
            SqlStatement? afterCount;
            SqlStatement? afterInsert;

            using (SqlCaptureScope scope = SqlCaptureScope.Begin())
            {
                Assert.Same(scope, SqlCaptureScope.Current);
                await Task.Yield();
                await repository.CountAsync(p => p.Department == department);
                afterCount = scope.LastStatement;
                Assert.NotNull(afterCount);
                Assert.Contains("COUNT", afterCount!.Sql, StringComparison.OrdinalIgnoreCase);
                Assert.Equal(afterCount.Sql, repository.LastExecutedSql);

                await Task.Delay(1);
                await repository.CreateAsync(InfrastructureTestData.NewPerson("capture-scope@example.com", department));
                afterInsert = scope.LastStatement;
                Assert.NotNull(afterInsert);
                Assert.Contains("INSERT", afterInsert!.Sql, StringComparison.OrdinalIgnoreCase);
                Assert.Contains("capture-scope@example.com", afterInsert.ToDebugString(), StringComparison.Ordinal);
            }

            Assert.Null(SqlCaptureScope.Current);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
            Assert.Null(repository.LastExecutedSql);
        }

        /// <summary>
        /// CreateWithQuery and ReadManyWithQueryAsync return the executed SQL along with the results.
        /// </summary>
        [Fact]
        public async Task ResultExtensions_ReturnQueryText()
        {
            const string department = "DiagWithQuery";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);

            IDurableResult<Person> created = repository.CreateWithQuery(InfrastructureTestData.NewPerson("with-query@example.com", department));
            Assert.False(string.IsNullOrWhiteSpace(created.Query));
            Assert.Contains("INSERT", created.Query, StringComparison.OrdinalIgnoreCase);
            Assert.Single(created.Result);

            IDurableResult<Person> read = await repository.ReadManyWithQueryAsync(p => p.Department == department);
            Assert.False(string.IsNullOrWhiteSpace(read.Query));
            Assert.Contains("SELECT", read.Query, StringComparison.OrdinalIgnoreCase);
            Assert.Single(read.Result);

            Assert.False(repository.CaptureSql);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// A string literal containing SQL injection text is bound as a parameter: neither BuildSql() nor
        /// LastExecutedSql contains any part of it, rows are filtered correctly and the table still exists.
        /// </summary>
        [Fact]
        public async Task Parameterization_InjectionLiteralNeverReachesSql()
        {
            const string department = "DiagInjection";
            ISqlRepository<Person> repository = _Provider.CreateRepository<Person>();
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
            Person hostile = InfrastructureTestData.NewPerson("injection-1@example.com", department);
            hostile.LastName = InjectionText;
            await repository.CreateAsync(hostile);
            await repository.CreateAsync(InfrastructureTestData.NewPerson("injection-2@example.com", department));

            string built = repository.Query().Where(p => p.LastName == "'; DROP TABLE people; --").BuildSql();
            Assert.DoesNotContain("DROP", built, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("--", built, StringComparison.Ordinal);

            repository.CaptureSql = true;
            List<Person> matches = new List<Person>();
            await foreach (Person person in repository.ReadManyAsync(p => p.LastName == "'; DROP TABLE people; --")) matches.Add(person);
            string? executed = repository.LastExecutedSql;
            Assert.NotNull(executed);
            Assert.DoesNotContain("DROP", executed!, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("--", executed!, StringComparison.Ordinal);

            Person match = Assert.Single(matches);
            Assert.Equal(InjectionText, match.LastName);
            Assert.Equal("injection-1@example.com", match.Email);

            Assert.Equal(0, await repository.CountAsync(p => p.LastName == "x' OR '1'='1"));
            Assert.Equal(2, await repository.CountAsync(p => p.Department == department));
            Assert.True(await repository.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM people") >= 2);
            await InfrastructureTestData.ClearDepartmentAsync(repository, department);
        }

        /// <summary>
        /// Disposes resources used by the test suite.
        /// </summary>
        public void Dispose()
        {
        }

        #endregion

        #region Private-Methods

        private string ExpectedDbSystem()
        {
            switch (_Provider.DatabaseType)
            {
                case TestDatabaseType.Sqlite: return "sqlite";
                case TestDatabaseType.Postgres: return "postgresql";
                case TestDatabaseType.MySql: return "mysql";
                case TestDatabaseType.SqlServer: return "mssql";
                default: throw new InvalidOperationException("Unknown provider " + _Provider.DatabaseType);
            }
        }

        #endregion
    }
}
