namespace Durable.DuckDb
{
    using System;
    using System.Collections.Generic;
    using DuckDB.NET.Data;
    using Durable.Sql;

    /// <summary>
    /// Connection settings for DuckDB repositories. DuckDB runs in-process: <see cref="DataSource"/> is a database file
    /// path, <c>:memory:</c> for an in-memory database private to one <see cref="DuckDbConnectionFactory"/>, or
    /// <c>:memory:?cache=shared</c> for the process-wide shared in-memory database. The server settings inherited from
    /// <see cref="RepositorySettings"/> (host, port, user, password, database) are not used.
    /// Thread safety: immutable after construction (init-only properties); safe to share.
    /// </summary>
    public sealed class DuckDbRepositorySettings : RepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// Gets <see cref="RepositoryType.DuckDb"/>.
        /// </summary>
        public override RepositoryType Type => RepositoryType.DuckDb;

        /// <summary>
        /// Gets the database file path, <c>:memory:</c> (private in-memory database) or <c>:memory:?cache=shared</c>
        /// (process-wide shared in-memory database). Default: null. Required by <see cref="BuildConnectionString"/>.
        /// </summary>
        public string? DataSource { get; init; }

        /// <summary>
        /// Gets the access mode (DuckDB <c>access_mode</c>). Default: null (DuckDB's default, automatic: read-write).
        /// A read-only file database can be opened by several processes at once; a read-write one by one process.
        /// </summary>
        public DuckDBAccessMode? AccessMode { get; init; }

        /// <summary>
        /// Gets the number of worker threads DuckDB uses (DuckDB <c>threads</c>). Default: null (DuckDB's default, the
        /// number of cores). Minimum: 1.
        /// </summary>
        public int? Threads { get; init; }

        /// <summary>
        /// Gets the memory limit (DuckDB <c>memory_limit</c>, for example "1GB"). Default: null (DuckDB's default, 80% of RAM).
        /// </summary>
        public string? MemoryLimit { get; init; }

        /// <summary>
        /// Gets whether <see cref="DataSource"/> names an in-memory database (<c>:memory:</c>, with or without options, or empty).
        /// </summary>
        public bool IsInMemory => IsInMemoryDataSource(DataSource);

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="DuckDbRepositorySettings"/> class.
        /// </summary>
        public DuckDbRepositorySettings()
        {
        }

        /// <summary>
        /// Returns settings for an in-memory database. The database is private to the connection factory built from these
        /// settings (every connection of that factory sees it) and is released when the factory is disposed.
        /// </summary>
        /// <returns>The settings.</returns>
        public static DuckDbRepositorySettings ForInMemory()
        {
            return new DuckDbRepositorySettings { DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource };
        }

        /// <summary>
        /// Returns settings for the process-wide shared in-memory database (<c>:memory:?cache=shared</c>): every factory and
        /// raw connection using it sees the same data while at least one connection to it is open.
        /// </summary>
        /// <returns>The settings.</returns>
        public static DuckDbRepositorySettings ForSharedInMemory()
        {
            return new DuckDbRepositorySettings { DataSource = DuckDBConnectionStringBuilder.InMemorySharedDataSource };
        }

        /// <summary>
        /// Returns settings for a database file, created when it does not exist.
        /// </summary>
        /// <param name="path">File path. Must not be null or empty.</param>
        /// <returns>The settings.</returns>
        /// <exception cref="ArgumentException">Thrown when path is null, empty or whitespace.</exception>
        public static DuckDbRepositorySettings ForFile(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("Path cannot be null or empty.", nameof(path));
            return new DuckDbRepositorySettings { DataSource = path };
        }

        /// <summary>
        /// Parses a DuckDB.NET connection string (for example <c>Data Source=analytics.duckdb;threads=4</c>).
        /// </summary>
        /// <param name="connectionString">Connection string. Must not be null.</param>
        /// <returns>The settings. Keywords without a typed property are kept in <see cref="RepositorySettings.AdditionalProperties"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null.</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty, whitespace or invalid.</exception>
        public static DuckDbRepositorySettings Parse(string connectionString)
        {
            ArgumentNullException.ThrowIfNull(connectionString);
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Connection string cannot be empty or whitespace", nameof(connectionString));

            DuckDBConnectionStringBuilder builder;
            try
            {
                builder = new DuckDBConnectionStringBuilder { ConnectionString = connectionString };
            }
            catch (Exception ex) when (ex is ArgumentException || ex is InvalidOperationException || ex is FormatException)
            {
                throw new ArgumentException("Invalid DuckDB connection string: " + ex.Message, nameof(connectionString), ex);
            }

            Dictionary<string, string>? additional = null;
            foreach (string key in builder.Keys)
            {
                string lower = key.ToLowerInvariant();
                if (lower == "datasource" || lower == "data source" || lower == "access_mode" || lower == "threads" || lower == "memory_limit") continue;
                additional ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                additional[key] = builder[key]?.ToString() ?? string.Empty;
            }

            return new DuckDbRepositorySettings
            {
                DataSource = builder.DataSource,
                AccessMode = builder.AccessMode,
                Threads = builder.Threads,
                MemoryLimit = string.IsNullOrEmpty(builder.MemoryLimit) ? null : builder.MemoryLimit,
                AdditionalProperties = additional
            };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a DuckDB.NET connection string from the settings.
        /// </summary>
        /// <returns>The connection string. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <see cref="DataSource"/> is null or empty, or <see cref="Threads"/> is less than 1.</exception>
        public override string BuildConnectionString()
        {
            if (string.IsNullOrWhiteSpace(DataSource))
                throw new InvalidOperationException("DataSource is required for a DuckDB connection string (a file path or :memory:).");
            if (Threads.HasValue && Threads.Value < 1)
                throw new InvalidOperationException("Threads must be at least 1.");

            DuckDBConnectionStringBuilder builder = new DuckDBConnectionStringBuilder { DataSource = DataSource };
            if (AccessMode.HasValue) builder.AccessMode = AccessMode.Value;
            if (Threads.HasValue) builder.Threads = Threads.Value;
            if (!string.IsNullOrEmpty(MemoryLimit)) builder.MemoryLimit = MemoryLimit;
            if (AdditionalProperties != null)
            {
                foreach (KeyValuePair<string, string> kvp in AdditionalProperties) builder[kvp.Key] = kvp.Value;
            }

            return builder.ConnectionString;
        }

        /// <summary>
        /// Returns whether a data source names an in-memory database: null, empty, <c>:memory:</c> or <c>:memory:?...</c>.
        /// </summary>
        /// <param name="dataSource">Data source; may be null.</param>
        /// <returns>True for an in-memory database.</returns>
        public static bool IsInMemoryDataSource(string? dataSource)
        {
            if (string.IsNullOrWhiteSpace(dataSource)) return true;
            string trimmed = dataSource.Trim();
            return string.Equals(trimmed, DuckDBConnectionStringBuilder.InMemoryDataSource, StringComparison.OrdinalIgnoreCase)
                || trimmed.StartsWith(DuckDBConnectionStringBuilder.InMemoryDataSource + "?", StringComparison.OrdinalIgnoreCase);
        }

        #endregion
    }
}
