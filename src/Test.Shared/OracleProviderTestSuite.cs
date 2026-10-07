namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Oracle;
    using Durable.Sql;
    using Xunit;

    /// <summary>
    /// Oracle-only coverage for what the shared suites cannot see: the stored representation of each mapped type
    /// (NUMBER(1), RAW(16) in RFC 4122 byte order, CLOB, BLOB, INTERVAL, DATE, TIMESTAMP WITH TIME ZONE), the documented
    /// empty-string-as-NULL behavior, IN lists above Oracle's 1000-item limit, array-bound bulk insert, upper-case
    /// identifier folding versus the case-preserving dialect, and SQL*Plus-style migration scripts.
    /// </summary>
    public class OracleProviderTestSuite
    {
        #region Private-Members

        private readonly IRepositoryProvider _Provider;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleProviderTestSuite"/> class.
        /// </summary>
        /// <param name="provider">The Oracle repository provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
        public OracleProviderTestSuite(IRepositoryProvider provider)
        {
            _Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Every mapped type round-trips, and the stored representation is the documented one: booleans are 1/0, GUIDs are
        /// RAW(16) whose hex equals the GUID's "N" text, long text is a CLOB and long binary a BLOB.
        /// </summary>
        [Fact]
        public async Task TypesRoundTripWithDocumentedStorage()
        {
            ISqlRepository<OraTypeItem> repository = await ResetAsync();
            Guid token = Guid.NewGuid();
            string longText = new string('x', 9000) + "日本";
            byte[] longBytes = Enumerable.Range(0, 5000).Select(i => (byte)(i % 251)).ToArray();
            DateTimeOffset happened = new DateTimeOffset(2024, 2, 29, 13, 14, 15, TimeSpan.FromHours(5.5)).AddTicks(1234567);
            OraTypeItem created = await repository.CreateAsync(new OraTypeItem
            {
                Name = "types", Code = "C-1", Note = longText, Flag = true, Token = token, Data = longBytes,
                Duration = TimeSpan.FromDays(-400).Add(TimeSpan.FromTicks(-3)), Day = new DateOnly(1999, 12, 31),
                TimeOfDay = new TimeOnly(23, 59, 58, 999), Happened = happened, Amount = 12345678901234567.0123456789m
            });
            Assert.True(created.Id > 0);

            OraTypeItem? stored = await repository.ReadByIdAsync(created.Id);
            Assert.NotNull(stored);
            Assert.Equal("C-1", stored!.Code);
            Assert.Equal(longText, stored.Note);
            Assert.True(stored.Flag);
            Assert.Equal(token, stored.Token);
            Assert.Equal(longBytes, stored.Data);
            Assert.Equal(TimeSpan.FromDays(-400).Add(TimeSpan.FromTicks(-3)), stored.Duration);
            Assert.Equal(new DateOnly(1999, 12, 31), stored.Day);
            Assert.Equal(new TimeOnly(23, 59, 58, 999), stored.TimeOfDay);
            Assert.Equal(happened, stored.Happened);
            Assert.Equal(happened.Offset, stored.Happened.Offset);
            Assert.Equal(12345678901234567.0123456789m, stored.Amount);

            Assert.Equal(token.ToString("N").ToUpperInvariant(), repository.ExecuteScalarRaw<string>("SELECT RAWTOHEX(token) FROM ora_type_items WHERE id = {0}", new object?[] { created.Id }));
            Assert.Equal(1L, repository.ExecuteScalarRaw<long>("SELECT flag FROM ora_type_items WHERE id = {0}", new object?[] { created.Id }));
            Assert.Equal(9002L, repository.ExecuteScalarRaw<long>("SELECT DBMS_LOB.GETLENGTH(note) FROM ora_type_items WHERE id = {0}", new object?[] { created.Id }));
            Assert.Equal(1L, await repository.CountAsync(x => x.Token == token && x.Flag && x.Day == new DateOnly(1999, 12, 31)));
            Assert.Equal(1L, await repository.CountAsync(x => x.Note!.Contains("日本")));

            string columns = string.Join(",", repository.FromSqlRaw<OraColumnType>("SELECT column_name AS name, data_type AS data_type FROM user_tab_columns WHERE table_name = 'ORA_TYPE_ITEMS' ORDER BY column_id")
                .Select(c => c.Name + " " + c.DataType));
            Assert.Contains("FLAG NUMBER", columns);
            Assert.Contains("TOKEN RAW", columns);
            Assert.Contains("NOTE CLOB", columns);
            Assert.Contains("DATA BLOB", columns);
            Assert.Contains("DURATION INTERVAL DAY(9) TO SECOND(7)", columns);
            Assert.Contains("DAY DATE", columns);
            Assert.Contains("HAPPENED TIMESTAMP(7) WITH TIME ZONE", columns);
        }

        /// <summary>
        /// Oracle stores an empty string as NULL: the dialect says so, the repository lacks
        /// <see cref="RepositoryCapabilities.EmptyStrings"/>, an empty string reads back as null from a nullable column and
        /// as the property initializer from a non-nullable one, and comparing with an empty string matches no row.
        /// </summary>
        [Fact]
        public async Task EmptyStringIsStoredAsNull()
        {
            ISqlRepository<OraTypeItem> repository = await ResetAsync();
            Assert.True(repository.Dialect.TreatsEmptyStringAsNull);
            Assert.Equal(RepositoryCapabilities.All & ~RepositoryCapabilities.EmptyStrings, repository.Capabilities);

            OraTypeItem created = repository.Create(new OraTypeItem { Name = string.Empty, Code = string.Empty, Day = new DateOnly(2020, 1, 1) });
            OraTypeItem? stored = repository.ReadById(created.Id);
            Assert.NotNull(stored);
            Assert.Null(stored!.Code);
            Assert.Equal(string.Empty, stored.Name);
            Assert.Equal(1L, repository.ExecuteScalarRaw<long>("SELECT COUNT(*) FROM ora_type_items WHERE code IS NULL AND name IS NULL"));
            Assert.Equal(0L, repository.Count(x => x.Code == string.Empty));
            Assert.Equal(1L, repository.Count(x => x.Code == null));
            Assert.Equal(1L, repository.Count(x => string.IsNullOrEmpty(x.Code)));
        }

        /// <summary>
        /// A Contains list longer than Oracle's 1000-item IN limit is split into OR-ed lists (AND-ed for NOT IN).
        /// </summary>
        [Fact]
        public async Task LongInListsAreSplit()
        {
            ISqlRepository<OraTypeItem> repository = await ResetAsync();
            List<OraTypeItem> rows = Enumerable.Range(0, 30).Select(i => new OraTypeItem { Name = "in" + i, Day = new DateOnly(2020, 1, 1) }).ToList();
            await repository.CreateManyAsync(rows);
            List<long> ids = rows.Select(r => r.Id).Concat(Enumerable.Range(1_000_000, 2_500).Select(i => (long)i)).ToList();
            List<string> names = rows.Select(r => r.Name).Concat(Enumerable.Range(0, 2_500).Select(i => "missing" + i.ToString(CultureInfo.InvariantCulture))).ToList();

            repository.CaptureSql = true;
            Assert.Equal(30L, await repository.CountAsync(x => ids.Contains(x.Id)));
            Assert.Contains(" OR ", repository.LastExecutedSql ?? string.Empty, StringComparison.Ordinal);
            Assert.Equal(30L, await repository.CountAsync(x => names.Contains(x.Name)));
            Assert.Equal(0L, await repository.CountAsync(x => !names.Contains(x.Name)));
        }

        /// <summary>
        /// Bulk insert (ODP.NET array binding, one INSERT per chunk) stores every row, including nulls, CLOBs and BLOBs, on the
        /// asynchronous and synchronous paths.
        /// </summary>
        [Fact]
        public async Task BulkInsertUsesArrayBinding()
        {
            ISqlRepository<OraTypeItem> repository = await ResetAsync();
            List<OraTypeItem> rows = Enumerable.Range(0, 1_200).Select(i => new OraTypeItem
            {
                Name = "bulk" + i,
                Code = i % 3 == 0 ? null : "c" + i,
                Note = i % 100 == 0 ? new string('n', 5000) : null,
                Data = i % 250 == 0 ? new byte[] { 1, 2, (byte)(i % 256) } : null,
                Flag = i % 2 == 0,
                Token = Guid.NewGuid(),
                Day = new DateOnly(2020, 1, 1).AddDays(i),
                Happened = DateTimeOffset.UnixEpoch.AddMinutes(i),
                Amount = i / 4m
            }).ToList();

            Assert.Equal(1_200L, await repository.BulkInsertAsync(rows));
            Assert.Equal(1_200L, await repository.CountAsync());
            Assert.Equal(400L, await repository.CountAsync(x => x.Code == null));
            Assert.Equal(12L, await repository.CountAsync(x => x.Note != null));
            Assert.Equal(600L, await repository.CountAsync(x => x.Flag));
            Assert.Equal(1_199m / 4m, await repository.MaxAsync(x => x.Amount));
            Assert.Equal(1_200L, repository.BulkInsert(rows.Take(1_200).Select(r => new OraTypeItem { Name = r.Name + "-sync", Day = r.Day, Token = r.Token })));
            Assert.Equal(2_400L, await repository.CountAsync());
        }

        /// <summary>
        /// The default dialect folds identifiers to upper case, so unquoted hand-written SQL reaches the mapped tables and
        /// introspection reports the folded names in lower case.
        /// </summary>
        [Fact]
        public async Task DefaultDialectFoldsIdentifiersToUpperCase()
        {
            ISqlRepository<OraTypeItem> repository = await ResetAsync();
            repository.Create(new OraTypeItem { Name = "fold", Day = new DateOnly(2020, 1, 1) });
            Assert.Equal("\"ORA_TYPE_ITEMS\"", OracleDialect.Default.QuoteIdentifier("ora_type_items"));
            Assert.Equal(1L, repository.ExecuteScalarRaw<long>("SELECT COUNT(*) FROM ora_type_items WHERE name = 'fold'"));
            Assert.Equal(1L, repository.ExecuteScalarRaw<long>("SELECT COUNT(*) FROM \"ORA_TYPE_ITEMS\""));

            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, _Provider.Dialect);
            TableSchema? table = reader.ReadTable("ora_type_items");
            Assert.NotNull(table);
            Assert.Equal("id", table!.Columns[0].Name);
            Assert.Contains(reader.ReadTableNames(), n => n == "ora_type_items");
        }

        /// <summary>
        /// With <c>new OracleDialect(upperCaseIdentifiers: false)</c> mixed-case names are kept as quoted, CRUD works, and
        /// introspection reports the names as stored.
        /// </summary>
        [Fact]
        public async Task CasePreservingDialectKeepsMixedCaseNames()
        {
            OracleDialect dialect = new OracleDialect(upperCaseIdentifiers: false);
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            using OracleRepository<OraMixedCaseItem> repository = new OracleRepository<OraMixedCaseItem>(factory, dialect);
            await repository.ExecuteSqlRawAsync(OracleRepositoryProvider.DropTableIfExistsSql("\"OraMixedCase\""));
            try
            {
                await repository.InitializeTableAsync(typeof(OraMixedCaseItem));
                OraMixedCaseItem created = await repository.CreateAsync(new OraMixedCaseItem { DisplayName = "Mixed" });
                Assert.True(created.ItemId > 0);
                Assert.Equal("Mixed", (await repository.ReadFirstAsync(x => x.DisplayName == "Mixed"))!.DisplayName);
                Assert.Equal(1L, repository.ExecuteScalarRaw<long>("SELECT COUNT(*) FROM \"OraMixedCase\" WHERE \"DisplayName\" = 'Mixed'"));

                DatabaseSchemaReader reader = new DatabaseSchemaReader(factory, dialect);
                TableSchema? table = reader.ReadTable("OraMixedCase");
                Assert.NotNull(table);
                Assert.Equal(new[] { "ItemId", "DisplayName" }, table!.Columns.Select(c => c.Name).ToArray());
                Assert.True(new SqlMigrator(factory, dialect).DiffSchema(new[] { typeof(OraMixedCaseItem) }).IsEmpty);
            }
            finally
            {
                await repository.ExecuteSqlRawAsync(OracleRepositoryProvider.DropTableIfExistsSql("\"OraMixedCase\""));
            }
        }

        /// <summary>
        /// Generated scripts end PL/SQL blocks with a "/" line and other statements with a semicolon, for SQL*Plus, SQLcl and
        /// SQL Developer.
        /// </summary>
        [Fact]
        public async Task ScriptsTerminatePlSqlBlocksWithSlash()
        {
            ISqlRepository<OraTypeItem> repository = _Provider.CreateRepository<OraTypeItem>();
            await repository.ExecuteSqlRawAsync(RelTestHelpers.DropTableSql(repository.Dialect, "ora_type_items"));
            await using IConnectionFactory factory = _Provider.CreateConnectionFactory();
            string script = new SqlMigrator(factory, _Provider.Dialect).DiffSchema(new[] { typeof(OraTypeItem) }).ToScript();
            Assert.Contains("BEGIN EXECUTE IMMEDIATE 'CREATE TABLE \"ORA_TYPE_ITEMS\"", script, StringComparison.Ordinal);
            Assert.Contains("END;" + Environment.NewLine + "/" + Environment.NewLine, script, StringComparison.Ordinal);
            Assert.DoesNotContain(";;", script, StringComparison.Ordinal);
        }

        #endregion

        #region Private-Methods

        private async Task<ISqlRepository<OraTypeItem>> ResetAsync()
        {
            ISqlRepository<OraTypeItem> repository = _Provider.CreateRepository<OraTypeItem>();
            await RelTestHelpers.RecreateTableAsync(repository);
            return repository;
        }

        #endregion
    }
}
