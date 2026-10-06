namespace Durable.LiteDb
{
    using System;
    using System.Text.Json;
    using Microsoft.Extensions.Logging;
    using Durable;
    using LiteDB;

    /// <summary>
    /// Settings for a <see cref="LiteDbBackend"/>: where the data lives (an existing <see cref="LiteDatabase"/>, a data
    /// file, or <see cref="InMemoryFilename"/> for a database held in memory), how a data file is opened (password,
    /// connection type, lock timeout, read-only mode, initial size), and how the backend behaves (JSON columns, logging).
    /// New databases are created with an ordinal (binary) string collation so that string comparisons, keys and indexes
    /// behave like C# ordinal comparisons; the collation of an existing data file is never changed (see
    /// <see cref="LiteDbBackend.HasOrdinalCollation"/>).
    /// <para>
    /// Location: <see cref="Database"/>, or <see cref="Filename"/> (default <see cref="InMemoryFilename"/>). A database
    /// passed in is never disposed by Durable; the file options (<see cref="Password"/>, <see cref="ConnectionType"/>,
    /// <see cref="Timeout"/>, <see cref="ReadOnly"/>, <see cref="InitialSizeBytes"/>, <see cref="Upgrade"/>) then do not
    /// apply. A database opened from <see cref="Filename"/> is owned and disposed by the backend.
    /// </para>
    /// Thread safety: not thread-safe; configure before creating the backend. The backend reads the settings once, when it
    /// is created; later changes have no effect on it.
    /// </summary>
    public sealed class LiteDbRepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// The filename that selects a database held in memory (a <see cref="System.IO.MemoryStream"/>), discarded when the
        /// backend is disposed.
        /// </summary>
        public const string InMemoryFilename = ":memory:";

        /// <summary>
        /// Gets or sets an existing LiteDB database to store data in; null to open one from <see cref="Filename"/>. The
        /// backend never disposes a database passed here; dispose it after the backend and its repositories.
        /// Default: null.
        /// </summary>
        public LiteDatabase? Database { get; set; }

        /// <summary>
        /// Gets or sets the data file path, or <see cref="InMemoryFilename"/> for an in-memory database. Ignored when
        /// <see cref="Database"/> is set.
        /// Default: <see cref="InMemoryFilename"/>.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when value is null.</exception>
        /// <exception cref="ArgumentException">Thrown when value is empty or whitespace.</exception>
        public string Filename
        {
            get => _Filename;
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("Filename cannot be empty.", nameof(value));
                _Filename = value;
            }
        }

        /// <summary>
        /// Gets whether the settings select a database held only in memory and discarded on dispose (no
        /// <see cref="Database"/> and <see cref="Filename"/> is <see cref="InMemoryFilename"/>).
        /// </summary>
        public bool IsInMemory => Database == null && string.Equals(_Filename, InMemoryFilename, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Gets or sets the password used to encrypt the data file (AES); null for an unencrypted file.
        /// Default: null.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when value is empty.</exception>
        public string? Password
        {
            get => _Password;
            set
            {
                if (value != null && value.Length == 0) throw new ArgumentException("Password cannot be empty; use null for no password.", nameof(value));
                _Password = value;
            }
        }

        /// <summary>
        /// Gets or sets how the data file is opened. Ignored for in-memory databases.
        /// Default: <see cref="LiteDbConnectionType.Direct"/>.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is not a defined connection type.</exception>
        public LiteDbConnectionType ConnectionType
        {
            get => _ConnectionType;
            set
            {
                if (value != LiteDbConnectionType.Direct && value != LiteDbConnectionType.Shared)
                    throw new ArgumentOutOfRangeException(nameof(value), "Unknown connection type " + value + ".");
                _ConnectionType = value;
            }
        }

        /// <summary>
        /// Gets or sets how long an operation waits for a lock (for example a collection written by an open transaction)
        /// before failing. LiteDB stores it in whole seconds (the value is rounded down) as a pragma of the data file; it is
        /// applied when the backend opens a writable database.
        /// Default: 1 minute. Minimum: 1 second. Maximum: 1 hour.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is outside the allowed range.</exception>
        public TimeSpan Timeout
        {
            get => _Timeout;
            set
            {
                if (value < TimeSpan.FromSeconds(1) || value > TimeSpan.FromHours(1))
                    throw new ArgumentOutOfRangeException(nameof(value), "Timeout must be between 1 second and 1 hour.");
                _Timeout = value;
            }
        }

        /// <summary>
        /// Gets or sets whether the data file is opened read-only (writes fail). Ignored for in-memory databases.
        /// Default: false.
        /// </summary>
        public bool ReadOnly { get; set; } = false;

        /// <summary>
        /// Gets or sets the initial size, in bytes, of a newly created data file; 0 lets the file grow on demand.
        /// Default: 0. Minimum: 0.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when value is negative.</exception>
        public long InitialSizeBytes
        {
            get => _InitialSizeBytes;
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException(nameof(value), "InitialSizeBytes cannot be negative.");
                _InitialSizeBytes = value;
            }
        }

        /// <summary>
        /// Gets or sets whether a LiteDB v4 data file is upgraded to the v5 format when opened.
        /// Default: false.
        /// </summary>
        public bool Upgrade { get; set; } = false;

        /// <summary>
        /// Gets or sets the logger for query plans (Debug) and index maintenance problems (Warning); null for none.
        /// Default: null.
        /// </summary>
        public ILogger? Logger { get; set; }

        /// <summary>
        /// Gets or sets the JSON options used to store JSON columns; null for camelCase, non-indented output (the same as
        /// the SQL providers). Under Native AOT, pass options with a source-generated context
        /// (<see cref="DurableJson.CreateOptions"/>).
        /// Default: null.
        /// </summary>
        public JsonSerializerOptions? JsonOptions { get; set; }

        #endregion

        #region Private-Members

        private string _Filename = InMemoryFilename;
        private string? _Password = null;
        private LiteDbConnectionType _ConnectionType = LiteDbConnectionType.Direct;
        private TimeSpan _Timeout = TimeSpan.FromMinutes(1);
        private long _InitialSizeBytes = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates settings for an in-memory database.
        /// </summary>
        public LiteDbRepositorySettings()
        {
        }

        /// <summary>
        /// Creates settings for an in-memory database, discarded when the backend is disposed.
        /// </summary>
        /// <returns>The settings. Never null.</returns>
        public static LiteDbRepositorySettings ForInMemory()
        {
            return new LiteDbRepositorySettings();
        }

        /// <summary>
        /// Creates settings for a data file (created when missing).
        /// </summary>
        /// <param name="filename">Data file path. Must not be null or empty.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filename is null.</exception>
        /// <exception cref="ArgumentException">Thrown when filename is empty or whitespace.</exception>
        public static LiteDbRepositorySettings ForFile(string filename)
        {
            return new LiteDbRepositorySettings { Filename = filename };
        }

        /// <summary>
        /// Creates settings for an existing LiteDB database, which the backend does not dispose.
        /// </summary>
        /// <param name="database">Database. Must not be null.</param>
        /// <returns>The settings. Never null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when database is null.</exception>
        public static LiteDbRepositorySettings ForDatabase(LiteDatabase database)
        {
            ArgumentNullException.ThrowIfNull(database);
            return new LiteDbRepositorySettings { Database = database };
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates the settings. <see cref="LiteDbBackend.Create"/> calls it.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when <see cref="Database"/> is combined with file options (a filename other than <see cref="InMemoryFilename"/>, a password, read-only mode or a shared connection).</exception>
        public void Validate()
        {
            if (Database == null) return;
            if (!string.Equals(_Filename, InMemoryFilename, StringComparison.OrdinalIgnoreCase) || _Password != null || ReadOnly || _ConnectionType != LiteDbConnectionType.Direct)
                throw new ArgumentException("LiteDB settings must set either Database or the file options (Filename, Password, ReadOnly, ConnectionType), not both.");
        }

        /// <summary>
        /// Builds the LiteDB connection string for the file options. New databases are created with the binary (ordinal)
        /// collation. Not used when <see cref="Database"/> is set.
        /// </summary>
        /// <returns>The connection string. Never null.</returns>
        public ConnectionString ToConnectionString()
        {
            bool memory = string.Equals(_Filename, InMemoryFilename, StringComparison.OrdinalIgnoreCase);
            return new ConnectionString
            {
                Filename = _Filename,
                Password = _Password,
                Connection = _ConnectionType == LiteDbConnectionType.Shared && !memory ? LiteDB.ConnectionType.Shared : LiteDB.ConnectionType.Direct,
                ReadOnly = ReadOnly && !memory,
                InitialSize = _InitialSizeBytes,
                Upgrade = Upgrade,
                Collation = Collation.Binary
            };
        }

        /// <inheritdoc />
        public override string ToString()
        {
            if (Database != null) return "LiteDB (existing database)";
            return "LiteDB " + (IsInMemory ? "in-memory" : "'" + _Filename + "'") + " (" + _ConnectionType + (ReadOnly ? ", read-only" : string.Empty) + (_Password != null ? ", encrypted" : string.Empty) + ")";
        }

        #endregion
    }
}
