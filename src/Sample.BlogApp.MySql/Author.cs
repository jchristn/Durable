namespace Sample.BlogApp.MySql
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Durable;

    /// <summary>
    /// Represents an author who can write blog posts.
    /// </summary>
    [Entity("authors")]
    public class Author
    {

        #region Public-Members

        /// <summary>
        /// Gets or sets the unique identifier for the author.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the username of the author.
        /// </summary>
        [Property("username", Flags.String, 50)]
        [Required(ErrorMessage = "Username is required")]
        [StringLength(50, MinimumLength = 3, ErrorMessage = "Username must be between 3 and 50 characters")]
        public string Username { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the email address of the author.
        /// </summary>
        [Property("email", Flags.String, 100)]
        [Required(ErrorMessage = "Email is required")]
        [EmailAddress(ErrorMessage = "Please provide a valid email address")]
        public string Email { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the full name of the author.
        /// </summary>
        [Property("full_name", Flags.String, 100)]
        [StringLength(100, ErrorMessage = "Full name cannot exceed 100 characters")]
        public string FullName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the biography of the author.
        /// </summary>
        [Property("bio", Flags.String, 500)]
        [StringLength(500, ErrorMessage = "Biography cannot exceed 500 characters")]
        public string Bio { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the date when the author joined.
        /// </summary>
        [Property("joined_date")]
        public DateTime JoinedDate { get; set; }

        /// <summary>
        /// Gets or sets whether the author account is active.
        /// Default: true. Inactive authors cannot publish new posts.
        /// </summary>
        [Property("is_active")]
        public bool IsActive { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns a string representation of the author.
        /// </summary>
        /// <returns>A formatted string containing the author's details.</returns>
        public override string ToString()
        {
            return $"Author({Id}): {Username} - {FullName} ({Email})";
        }

        #endregion

        #region Private-Methods

        #endregion

    }

}
