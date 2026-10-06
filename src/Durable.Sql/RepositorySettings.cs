namespace Durable.Sql
{

    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Strongly-typed connection settings for a SQL provider (<c>SqliteRepositorySettings</c>, <c>MySqlRepositorySettings</c>,
    /// <c>PostgresRepositorySettings</c>, <c>SqlServerRepositorySettings</c>). Each provider adds a static <c>Parse</c> and
    /// implements <see cref="BuildConnectionString"/>; pass an instance to the provider's repository or connection factory
    /// constructor instead of a connection string.
    /// Thread safety: properties are init-only, so an instance is immutable after construction and safe to share.
    /// </summary>
    public abstract class RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// Gets the provider these settings are for (for example <see cref="RepositoryType.Postgres"/>). Never null.
        /// </summary>
        public abstract RepositoryType Type { get; }

        /// <summary>
        /// Gets the database server host name or address. Default: null. Required by the server providers; unused by SQLite.
        /// </summary>
        public string? Hostname { get; init; }

        /// <summary>
        /// Gets the server port. Default: null (the provider's default port: 3306, 5432 or 1433). Minimum: 1. Maximum: 65535.
        /// </summary>
        public int? Port { get; init; }

        /// <summary>
        /// Gets the user name for password authentication. Default: null (no user name is written).
        /// </summary>
        public string? Username { get; init; }

        /// <summary>
        /// Gets the password for password authentication. Default: null (no password is written).
        /// </summary>
        public string? Password { get; init; }

        /// <summary>
        /// Gets the database (catalog) name. Default: null. Required by PostgreSQL and SQL Server; optional for MySQL;
        /// unused by SQLite (see <c>SqliteRepositorySettings.DataSource</c>).
        /// </summary>
        public string? Database { get; init; }

        /// <summary>
        /// Gets additional driver connection-string keywords, written verbatim after the typed settings (later values win).
        /// Default: null (none). Parse collects keywords it has no typed property for here.
        /// </summary>
        public IReadOnlyDictionary<string, string>? AdditionalProperties { get; init; }

        #endregion


        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the base settings.
        /// </summary>
        protected RepositorySettings()
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds the driver connection string from the settings.
        /// </summary>
        /// <returns>The connection string. Never null.</returns>
        /// <exception cref="InvalidOperationException">Thrown when a setting the provider requires is missing.</exception>
        public abstract string BuildConnectionString();

        #endregion


    }

}
