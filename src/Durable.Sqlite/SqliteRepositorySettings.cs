namespace Durable.Sqlite
{

    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;
    using System.Text;
    using Durable;
    using Microsoft.Data.Sqlite;

    /// <summary>
    /// Connection settings for SQLite repositories
    /// </summary>
    public sealed class SqliteRepositorySettings : RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// The type of repository
        /// </summary>
        public override RepositoryType Type => RepositoryType.Sqlite;

        /// <summary>
        /// The file path to the SQLite database file. Required for file-based databases.
        /// </summary>
        public string? DataSource { get; init; }

        /// <summary>
        /// The cache mode. Default: null (uses SQLite default). Values: Default, Private, Shared
        /// </summary>
        public SqliteCacheMode? CacheMode { get; init; }

        /// <summary>
        /// The open mode. Default: null (uses SQLite default). Values: ReadWriteCreate, ReadWrite, ReadOnly, Memory
        /// </summary>
        public SqliteOpenMode? Mode { get; init; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqliteRepositorySettings class
        /// </summary>
        public SqliteRepositorySettings()
        {
        }

        /// <summary>
        /// Parses a SQLite connection string and returns a SqliteRepositorySettings instance
        /// </summary>
        /// <param name="connectionString">The connection string to parse</param>
        /// <returns>A SqliteRepositorySettings instance</returns>
        /// <exception cref="ArgumentNullException">Thrown when connectionString is null</exception>
        /// <exception cref="ArgumentException">Thrown when connectionString is empty or whitespace, or when the connection string is invalid</exception>
        public static SqliteRepositorySettings Parse(string connectionString)
        {
            ArgumentNullException.ThrowIfNull(connectionString);

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Connection string cannot be empty or whitespace", nameof(connectionString));
            }

            SqliteConnectionStringBuilder builder;

            try
            {
                builder = new SqliteConnectionStringBuilder(connectionString);
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Invalid SQLite connection string: {ex.Message}", nameof(connectionString), ex);
            }

            Dictionary<string, string>? additionalProperties = null;

            foreach (string key in builder.Keys)
            {
                string lowerKey = key.ToLowerInvariant();

                if (lowerKey != "data source" &&
                    lowerKey != "datasource" &&
                    lowerKey != "filename" &&
                    lowerKey != "cache" &&
                    lowerKey != "mode")
                {
                    if (additionalProperties == null)
                    {
                        additionalProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    }

                    additionalProperties[key] = builder[key]?.ToString() ?? string.Empty;
                }
            }

            return new SqliteRepositorySettings
            {
                DataSource = builder.DataSource,
                CacheMode = builder.Cache != SqliteCacheMode.Default ? builder.Cache : null,
                Mode = builder.Mode != SqliteOpenMode.ReadWriteCreate ? builder.Mode : null,
                AdditionalProperties = additionalProperties
            };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a SQLite connection string from the current settings
        /// </summary>
        /// <returns>A SQLite connection string</returns>
        /// <exception cref="InvalidOperationException">Thrown when DataSource is null or empty</exception>
        public override string BuildConnectionString()
        {
            if (string.IsNullOrWhiteSpace(DataSource))
            {
                throw new InvalidOperationException("DataSource is required for SQLite connection string");
            }

            SqliteConnectionStringBuilder builder = new SqliteConnectionStringBuilder
            {
                DataSource = DataSource
            };

            if (CacheMode.HasValue)
            {
                builder.Cache = CacheMode.Value;
            }

            if (Mode.HasValue)
            {
                builder.Mode = Mode.Value;
            }

            if (AdditionalProperties != null)
            {
                foreach (KeyValuePair<string, string> kvp in AdditionalProperties)
                {
                    builder[kvp.Key] = kvp.Value;
                }
            }

            return builder.ConnectionString;
        }

        #endregion

        #region Private-Methods

        #endregion

    }

}
