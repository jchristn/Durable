namespace Sample.BlogApp.SqlServer
{
    using System;

    /// <summary>
    /// Represents blog statistics for projection queries (DTO pattern).
    /// This demonstrates mapping query results to non-entity types.
    /// </summary>
    public class BlogStatistics
    {

        #region Public-Members

        /// <summary>
        /// Gets or sets the author's username.
        /// </summary>
        public string AuthorUsername { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the total number of posts by this author.
        /// </summary>
        public int TotalPosts { get; set; }

        /// <summary>
        /// Gets or sets the total number of published posts.
        /// </summary>
        public int PublishedPosts { get; set; }

        /// <summary>
        /// Gets or sets the total view count across all posts.
        /// </summary>
        public int TotalViews { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a string representation of the blog statistics.
        /// </summary>
        /// <returns>A formatted string containing the statistics.</returns>
        public override string ToString()
        {
            return $"Author: {AuthorUsername}, Posts: {TotalPosts} ({PublishedPosts} published), Views: {TotalViews}";
        }

        #endregion

        #region Private-Methods

        #endregion

    }

}
