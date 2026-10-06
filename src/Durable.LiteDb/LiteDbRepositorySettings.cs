namespace Durable.LiteDb
{
    using System;
    using LiteDB;

    /// <summary>
    /// Strongly-typed settings for opening a LiteDB database for a <see cref="LiteDbBackend"/>: the data file (or
    /// <see cref="InMemoryFilename"/> for a database held in memory), password, connection type, lock timeout, read-only
    /// mode and initial size. New databases are created with an ordinal (binary) string collation so that string
    /// comparisons, keys and indexes behave like C# ordinal comparisons; the collation of an existing data file is never
    /// changed (see <see cref="LiteDbBackend.HasOrdinalCollation"/>).
    /// Thread safety: not thread-safe while being configured; treat as immutable once passed to a backend.
    /// </summary>
    public class LiteDbRepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// The filename that selects a database held in memory (a <see cref="System.IO.MemoryStream"/>), discarded when the
        /// backend is disposed.
        /// </summary>
        public const string InMemoryFilename = ":memory:";

        /// <summary>
        /// Gets or sets the data file path, or <see cref="InMemoryFilename"/> for an in-memory database.
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
        /// Gets whether <see cref="Filename"/> selects an in-memory database.
        /// </summary>
        public bool IsInMemory => string.Equals(_Filename, InMemoryFilename, StringComparison.OrdinalIgnoreCase);

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
        /// Instantiates settings for a data file.
        /// </summary>
        /// <param name="filename">Data file path, or <see cref="InMemoryFilename"/>. Must not be null or empty.</param>
        /// <exception cref="ArgumentNullException">Thrown when filename is null.</exception>
        /// <exception cref="ArgumentException">Thrown when filename is empty or whitespace.</exception>
        public LiteDbRepositorySettings(string filename)
        {
            Filename = filename;
        }

        /// <summary>
        /// Creates settings for an in-memory database.
        /// </summary>
        /// <returns>The settings.</returns>
        public static LiteDbRepositorySettings InMemory()
        {
            return new LiteDbRepositorySettings(InMemoryFilename);
        }

        /// <summary>
        /// Creates settings for a data file.
        /// </summary>
        /// <param name="filename">Data file path. Must not be null or empty.</param>
        /// <returns>The settings.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filename is null.</exception>
        /// <exception cref="ArgumentException">Thrown when filename is empty or whitespace.</exception>
        public static LiteDbRepositorySettings ForFile(string filename)
        {
            return new LiteDbRepositorySettings(filename);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds the LiteDB connection string for these settings. New databases are created with the binary (ordinal)
        /// collation.
        /// </summary>
        /// <returns>The connection string. Never null.</returns>
        public ConnectionString ToConnectionString()
        {
            return new ConnectionString
            {
                Filename = _Filename,
                Password = _Password,
                Connection = _ConnectionType == LiteDbConnectionType.Shared && !IsInMemory ? LiteDB.ConnectionType.Shared : LiteDB.ConnectionType.Direct,
                ReadOnly = ReadOnly && !IsInMemory,
                InitialSize = _InitialSizeBytes,
                Upgrade = Upgrade,
                Collation = Collation.Binary
            };
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "LiteDB " + (IsInMemory ? "in-memory" : "'" + _Filename + "'") + " (" + _ConnectionType + (ReadOnly ? ", read-only" : string.Empty) + (_Password != null ? ", encrypted" : string.Empty) + ")";
        }

        #endregion
    }
}
