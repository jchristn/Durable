namespace Durable
{

    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Abstract base class for repository connection settings.
    /// Derived classes should implement provider-specific connection string parsing and building.
    /// </summary>
    public abstract class RepositorySettings
    {

        #region Public-Members

        /// <summary>
        /// The type of repository
        /// </summary>
        public abstract RepositoryType Type { get; }

        /// <summary>
        /// The hostname or server address. Default: null
        /// </summary>
        public string? Hostname { get; init; }

        /// <summary>
        /// The port number for the database server. Default: null (uses provider default)
        /// </summary>
        public int? Port { get; init; }

        /// <summary>
        /// The username for authentication. Default: null
        /// </summary>
        public string? Username { get; init; }

        /// <summary>
        /// The password for authentication. Default: null
        /// </summary>
        public string? Password { get; init; }

        /// <summary>
        /// The database name. Default: null
        /// </summary>
        public string? Database { get; init; }

        /// <summary>
        /// Additional provider-specific properties. Default: null
        /// </summary>
        public IReadOnlyDictionary<string, string>? AdditionalProperties { get; init; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the RepositorySettings class
        /// </summary>
        protected RepositorySettings()
        {
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a connection string from the current settings
        /// </summary>
        /// <returns>A connection string</returns>
        public abstract string BuildConnectionString();

        #endregion

        #region Private-Methods

        #endregion

    }

}
