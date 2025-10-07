namespace Durable
{

    using System;

    /// <summary>
    /// Represents a repository type in an extensible, type-safe manner.
    /// This class provides built-in repository types (Sqlite, MySql, Postgres, SqlServer)
    /// and allows users to define custom repository types.
    /// </summary>
    public sealed class RepositoryType : IEquatable<RepositoryType>
    {

        #region Public-Members

        /// <summary>
        /// The unique identifier for this repository type
        /// </summary>
        public string Identifier { get; }

        /// <summary>
        /// The display name for this repository type
        /// </summary>
        public string DisplayName { get; }

        /// <summary>
        /// Represents a SQLite repository
        /// </summary>
        public static readonly RepositoryType Sqlite = new RepositoryType("sqlite", "SQLite");

        /// <summary>
        /// Represents a MySQL repository
        /// </summary>
        public static readonly RepositoryType MySql = new RepositoryType("mysql", "MySQL");

        /// <summary>
        /// Represents a PostgreSQL repository
        /// </summary>
        public static readonly RepositoryType Postgres = new RepositoryType("postgres", "PostgreSQL");

        /// <summary>
        /// Represents a SQL Server repository
        /// </summary>
        public static readonly RepositoryType SqlServer = new RepositoryType("sqlserver", "SQL Server");

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Creates a new repository type with the specified identifier and display name
        /// </summary>
        /// <param name="identifier">The unique identifier for the repository type (case-insensitive)</param>
        /// <param name="displayName">The display name for the repository type</param>
        /// <exception cref="ArgumentNullException">Thrown when identifier or displayName is null</exception>
        /// <exception cref="ArgumentException">Thrown when identifier or displayName is empty or whitespace</exception>
        public RepositoryType(string identifier, string displayName)
        {
            ArgumentNullException.ThrowIfNull(identifier);
            ArgumentNullException.ThrowIfNull(displayName);

            if (string.IsNullOrWhiteSpace(identifier))
            {
                throw new ArgumentException("Identifier cannot be empty or whitespace", nameof(identifier));
            }

            if (string.IsNullOrWhiteSpace(displayName))
            {
                throw new ArgumentException("Display name cannot be empty or whitespace", nameof(displayName));
            }

            Identifier = identifier.ToLowerInvariant();
            DisplayName = displayName;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Determines whether this repository type is equal to another repository type
        /// </summary>
        /// <param name="other">The other repository type to compare</param>
        /// <returns>True if the repository types are equal, false otherwise</returns>
        public bool Equals(RepositoryType? other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return string.Equals(Identifier, other.Identifier, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Determines whether this repository type is equal to another object
        /// </summary>
        /// <param name="obj">The object to compare</param>
        /// <returns>True if the objects are equal, false otherwise</returns>
        public override bool Equals(object? obj)
        {
            return Equals(obj as RepositoryType);
        }

        /// <summary>
        /// Gets the hash code for this repository type
        /// </summary>
        /// <returns>The hash code</returns>
        public override int GetHashCode()
        {
            return StringComparer.OrdinalIgnoreCase.GetHashCode(Identifier);
        }

        /// <summary>
        /// Returns the display name of this repository type
        /// </summary>
        /// <returns>The display name</returns>
        public override string ToString()
        {
            return DisplayName;
        }

        /// <summary>
        /// Determines whether two repository types are equal
        /// </summary>
        /// <param name="left">The first repository type</param>
        /// <param name="right">The second repository type</param>
        /// <returns>True if the repository types are equal, false otherwise</returns>
        public static bool operator ==(RepositoryType? left, RepositoryType? right)
        {
            if (left is null)
            {
                return right is null;
            }

            return left.Equals(right);
        }

        /// <summary>
        /// Determines whether two repository types are not equal
        /// </summary>
        /// <param name="left">The first repository type</param>
        /// <param name="right">The second repository type</param>
        /// <returns>True if the repository types are not equal, false otherwise</returns>
        public static bool operator !=(RepositoryType? left, RepositoryType? right)
        {
            return !(left == right);
        }

        #endregion

        #region Private-Methods

        #endregion

    }

}
