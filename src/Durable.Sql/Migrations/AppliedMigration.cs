namespace Durable.Sql
{
    using System;

    /// <summary>
    /// A row of the migration history table.
    /// Thread safety: immutable; safe to share.
    /// </summary>
    public sealed class AppliedMigration
    {
        #region Public-Members

        /// <summary>
        /// Gets the migration identifier. Never null.
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// Gets the description recorded when applied; may be null.
        /// </summary>
        public string? Description { get; }

        /// <summary>
        /// Gets when the migration was applied (UTC).
        /// </summary>
        public DateTime AppliedUtc { get; }

        /// <summary>
        /// Gets how long the migration took, in milliseconds. Minimum: 0.
        /// </summary>
        public long DurationMs { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates a history record.
        /// </summary>
        /// <param name="id">Migration identifier. Must not be null or empty.</param>
        /// <param name="description">Description; may be null.</param>
        /// <param name="appliedUtc">Application time (UTC).</param>
        /// <param name="durationMs">Duration in milliseconds. Negative values are clamped to 0.</param>
        /// <exception cref="ArgumentException">Thrown when id is null or empty.</exception>
        public AppliedMigration(string id, string? description, DateTime appliedUtc, long durationMs)
        {
            if (string.IsNullOrEmpty(id)) throw new ArgumentException("Migration id cannot be null or empty.", nameof(id));
            Id = id;
            Description = description;
            AppliedUtc = appliedUtc.Kind == DateTimeKind.Utc ? appliedUtc : DateTime.SpecifyKind(appliedUtc, DateTimeKind.Utc);
            DurationMs = Math.Max(0, durationMs);
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns "Id (applied at ...)".
        /// </summary>
        /// <returns>The description.</returns>
        public override string ToString()
        {
            return Id + " (applied " + AppliedUtc.ToString("u", System.Globalization.CultureInfo.InvariantCulture) + ")";
        }

        #endregion
    }
}
