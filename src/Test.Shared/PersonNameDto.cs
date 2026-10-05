namespace Test.Shared
{
    /// <summary>
    /// Unattributed DTO used by raw SQL tests. Columns are returned in snake_case (<c>first_name</c>,
    /// <c>last_name</c>, <c>person_age</c>) and must map to these PascalCase properties by name.
    /// </summary>
    public class PersonNameDto
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the first name. Null when the column is absent or NULL.
        /// </summary>
        public string? FirstName { get; set; }

        /// <summary>
        /// Gets or sets the last name. Null when the column is absent or NULL.
        /// </summary>
        public string? LastName { get; set; }

        /// <summary>
        /// Gets or sets the age. Default: 0.
        /// </summary>
        public int PersonAge { get; set; }

        #endregion
    }
}
