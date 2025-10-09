namespace Sample.BlogApp.Postgres
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Durable;

    /// <summary>
    /// Represents a comment on a blog post.
    /// </summary>
    [Entity("comments")]
    public class Comment
    {

        #region Public-Members

        /// <summary>
        /// Gets or sets the unique identifier for the comment.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the foreign key reference to the blog post this comment belongs to.
        /// </summary>
        [Property("post_id")]
        [Required(ErrorMessage = "Post ID is required")]
        public int PostId { get; set; }

        /// <summary>
        /// Gets or sets the name of the commenter.
        /// </summary>
        [Property("commenter_name", Flags.String, 100)]
        [Required(ErrorMessage = "Commenter name is required")]
        [StringLength(100, MinimumLength = 2, ErrorMessage = "Commenter name must be between 2 and 100 characters")]
        public string CommenterName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the email of the commenter.
        /// </summary>
        [Property("commenter_email", Flags.String, 100)]
        [Required(ErrorMessage = "Commenter email is required")]
        [EmailAddress(ErrorMessage = "Please provide a valid email address")]
        public string CommenterEmail { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the content of the comment.
        /// </summary>
        [Property("content", Flags.String, 1000)]
        [Required(ErrorMessage = "Comment content is required")]
        [StringLength(1000, MinimumLength = 1, ErrorMessage = "Comment must be between 1 and 1000 characters")]
        public string Content { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether the comment is approved.
        /// Default: false. Unapproved comments should not be displayed publicly.
        /// </summary>
        [Property("is_approved")]
        public bool IsApproved { get; set; }

        /// <summary>
        /// Gets or sets the date and time when the comment was created.
        /// </summary>
        [Property("created_date")]
        public DateTime CreatedDate { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a string representation of the comment.
        /// </summary>
        /// <returns>A formatted string containing the comment's details.</returns>
        public override string ToString()
        {
            string status = IsApproved ? "Approved" : "Pending";
            string preview = Content.Length > 50 ? Content.Substring(0, 47) + "..." : Content;
            return $"Comment({Id}): by {CommenterName} on Post {PostId} [{status}] - \"{preview}\"";
        }

        #endregion

        #region Private-Methods

        #endregion

    }

}
