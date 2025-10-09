namespace Sample.BlogApp.Postgres
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Durable;

    /// <summary>
    /// Represents a blog post written by an author.
    /// </summary>
    [Entity("blog_posts")]
    public class BlogPost
    {

        #region Public-Members

        /// <summary>
        /// Gets or sets the unique identifier for the blog post.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the foreign key reference to the author who wrote this post.
        /// </summary>
        [Property("author_id")]
        [Required(ErrorMessage = "Author ID is required")]
        public int AuthorId { get; set; }

        /// <summary>
        /// Gets or sets the title of the blog post.
        /// </summary>
        [Property("title", Flags.String, 200)]
        [Required(ErrorMessage = "Title is required")]
        [StringLength(200, MinimumLength = 5, ErrorMessage = "Title must be between 5 and 200 characters")]
        public string Title { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the URL-friendly slug for the blog post.
        /// </summary>
        [Property("slug", Flags.String, 250)]
        [Required(ErrorMessage = "Slug is required")]
        [StringLength(250, ErrorMessage = "Slug cannot exceed 250 characters")]
        public string Slug { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the content of the blog post.
        /// </summary>
        [Property("content", Flags.String, 10000)]
        [Required(ErrorMessage = "Content is required")]
        public string Content { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the excerpt or summary of the blog post.
        /// </summary>
        [Property("excerpt", Flags.String, 500)]
        [StringLength(500, ErrorMessage = "Excerpt cannot exceed 500 characters")]
        public string Excerpt { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether the blog post is published.
        /// Default: false. Unpublished posts are drafts.
        /// </summary>
        [Property("is_published")]
        public bool IsPublished { get; set; }

        /// <summary>
        /// Gets or sets the number of views for this blog post.
        /// Default: 0. Minimum: 0.
        /// </summary>
        [Property("view_count")]
        [Range(0, int.MaxValue, ErrorMessage = "View count cannot be negative")]
        public int ViewCount { get; set; }

        /// <summary>
        /// Gets or sets the date and time when the post was created.
        /// </summary>
        [Property("created_date")]
        public DateTime CreatedDate { get; set; }

        /// <summary>
        /// Gets or sets the date and time when the post was last updated.
        /// </summary>
        [Property("updated_date")]
        public DateTime UpdatedDate { get; set; }

        /// <summary>
        /// Gets or sets the date and time when the post was published.
        /// </summary>
        [Property("published_date")]
        public DateTime? PublishedDate { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a string representation of the blog post.
        /// </summary>
        /// <returns>A formatted string containing the blog post's details.</returns>
        public override string ToString()
        {
            string status = IsPublished ? "Published" : "Draft";
            return $"BlogPost({Id}): '{Title}' by Author {AuthorId} [{status}] - {ViewCount} views";
        }

        #endregion

        #region Private-Methods

        #endregion

    }

}
