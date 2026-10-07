namespace Test.Shared
{
    using System;

    /// <summary>
    /// Describes how the Durable test suites should connect to the database under test.
    /// A single configuration is shared by the Touchstone CLI runner, the xUnit adapter, and the NUnit adapter.
    /// Defaults target an in-process SQLite database so that <c>dotnet test</c> works with no external dependencies.
    /// </summary>
    public sealed class TestRuntimeConfiguration
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the database provider to exercise. Default: <see cref="TestDatabaseType.Sqlite"/>.
        /// </summary>
        public TestDatabaseType DatabaseType { get; set; } = TestDatabaseType.Sqlite;

        /// <summary>
        /// Gets or sets the database (catalog) name for server-based providers. Default: "durable_touchstone".
        /// Ignored by in-process providers (SQLite, DuckDB) when <see cref="Filename"/> or an in-memory database is used.
        /// </summary>
        public string DatabaseName { get; set; } = "durable_touchstone";

        /// <summary>
        /// Gets or sets the database file path for in-process providers. When null (default), an in-memory database is used.
        /// Only applicable to <see cref="TestDatabaseType.Sqlite"/> and <see cref="TestDatabaseType.DuckDb"/>.
        /// </summary>
        public string? Filename { get; set; }

        /// <summary>
        /// Gets or sets the hostname for server-based providers and document backends. Nullable; required unless the
        /// target is in-process (<see cref="TestDatabaseTypes.IsInProcess"/>).
        /// </summary>
        public string? Hostname { get; set; }

        /// <summary>
        /// Gets or sets the TCP port for server-based providers. Nullable; provider defaults are applied when omitted.
        /// </summary>
        public int? Port { get; set; }

        /// <summary>
        /// Gets or sets the SQL Server named instance. Nullable; applicable only to <see cref="TestDatabaseType.SqlServer"/>.
        /// </summary>
        public string? Instance { get; set; }

        /// <summary>
        /// Gets or sets the username for server-based providers. Nullable.
        /// </summary>
        public string? Username { get; set; }

        /// <summary>
        /// Gets or sets the password for server-based providers. Nullable.
        /// </summary>
        public string? Password { get; set; }

        /// <summary>
        /// Gets or sets an optional schema name passed through to providers that support it. Nullable.
        /// </summary>
        public string? Schema { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether provider debug logging should be enabled. Default: false.
        /// </summary>
        public bool Debug { get; set; }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates a deep copy of this configuration.
        /// </summary>
        /// <returns>A new <see cref="TestRuntimeConfiguration"/> with identical values.</returns>
        public TestRuntimeConfiguration Copy()
        {
            return new TestRuntimeConfiguration
            {
                DatabaseType = DatabaseType,
                DatabaseName = DatabaseName,
                Filename = Filename,
                Hostname = Hostname,
                Port = Port,
                Instance = Instance,
                Username = Username,
                Password = Password,
                Schema = Schema,
                Debug = Debug
            };
        }

        /// <summary>
        /// Validates that the configuration contains the values required for the selected provider.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when required connection values are missing.</exception>
        public void Validate()
        {
            if (TestDatabaseTypes.IsInProcess(DatabaseType))
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(Hostname))
            {
                throw new InvalidOperationException("Hostname is required for " + TestDatabaseTypes.ProviderName(DatabaseType) + " automated test execution.");
            }

            if (string.IsNullOrWhiteSpace(DatabaseName))
            {
                throw new InvalidOperationException("Database name is required for " + TestDatabaseTypes.ProviderName(DatabaseType) + " automated test execution.");
            }
        }

        #endregion
    }
}
