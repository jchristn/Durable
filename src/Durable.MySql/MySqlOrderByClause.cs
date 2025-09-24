namespace Durable.MySql
{
    using System;

    /// <summary>
    /// Represents an ORDER BY clause in a MySQL query.
    /// Provides ordering direction and column information for query building.
    /// </summary>
    internal class MySqlOrderByClause
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the column name or expression to order by.
        /// </summary>
        public string Column { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the ordering direction (ASC or DESC).
        /// </summary>
        public OrderDirection Direction { get; set; } = OrderDirection.Ascending;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlOrderByClause.
        /// </summary>
        public MySqlOrderByClause()
        {
        }

        /// <summary>
        /// Initializes a new instance of the MySqlOrderByClause with the specified column and direction.
        /// </summary>
        /// <param name="column">The column name or expression to order by</param>
        /// <param name="direction">The ordering direction</param>
        /// <exception cref="ArgumentNullException">Thrown when column is null</exception>
        /// <exception cref="ArgumentException">Thrown when column is empty or whitespace</exception>
        public MySqlOrderByClause(string column, OrderDirection direction)
        {
            if (string.IsNullOrWhiteSpace(column))
                throw new ArgumentException("Column cannot be null or empty", nameof(column));

            Column = column;
            Direction = direction;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the SQL representation of this ORDER BY clause.
        /// </summary>
        /// <returns>The SQL ORDER BY clause string</returns>
        public override string ToString()
        {
            string directionStr = Direction == OrderDirection.Ascending ? "ASC" : "DESC";
            return $"`{Column}` {directionStr}";
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current MySqlOrderByClause.
        /// </summary>
        /// <param name="obj">The object to compare with the current object</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false</returns>
        public override bool Equals(object? obj)
        {
            if (obj is MySqlOrderByClause other)
            {
                return Column.Equals(other.Column, StringComparison.OrdinalIgnoreCase) &&
                       Direction == other.Direction;
            }
            return false;
        }

        /// <summary>
        /// Returns the hash code for this MySqlOrderByClause.
        /// </summary>
        /// <returns>A hash code for the current object</returns>
        public override int GetHashCode()
        {
            return HashCode.Combine(Column.ToLowerInvariant(), Direction);
        }

        #endregion
    }

    /// <summary>
    /// Specifies the direction of ordering for ORDER BY clauses.
    /// </summary>
    internal enum OrderDirection
    {
        /// <summary>
        /// Ascending order (smallest to largest).
        /// </summary>
        Ascending,

        /// <summary>
        /// Descending order (largest to smallest).
        /// </summary>
        Descending
    }
}