namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Represents a complex entity with various data types and properties.
    /// </summary>
    [Entity("complex_entities")]
    public class ComplexEntity
    {
        /// <summary>
        /// Gets or sets the unique identifier for the complex entity.
        /// </summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name of the complex entity.
        /// </summary>
        [Property("name", Flags.String, 100)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the date and time when the entity was created.
        /// </summary>
        [Property("created_date")]
        public DateTime CreatedDate { get; set; }

        /// <summary>
        /// Gets or sets the date and time when the entity was last updated.
        /// </summary>
        [Property("updated_date")]
        public DateTimeOffset? UpdatedDate { get; set; }

        /// <summary>
        /// Gets or sets the globally unique identifier for the entity.
        /// </summary>
        [Property("unique_id")]
        public Guid UniqueId { get; set; }

        /// <summary>
        /// Gets or sets the duration associated with the entity.
        /// </summary>
        [Property("duration")]
        public TimeSpan Duration { get; set; }

        /// <summary>
        /// Gets or sets the status of the entity.
        /// </summary>
        [Property("status")]
        public Status Status { get; set; }

        /// <summary>
        /// Gets or sets the status of the entity as an integer value.
        /// </summary>
        [Property("status_int")]
        public Status StatusAsInt { get; set; }

        /// <summary>
        /// Gets or sets the array of tags associated with the entity.
        /// </summary>
        [Property("tags")]
        public string[] Tags { get; set; }

        /// <summary>
        /// Gets or sets the list of scores associated with the entity.
        /// </summary>
        [Property("scores")]
        public List<int> Scores { get; set; }

        /// <summary>
        /// Gets or sets the metadata dictionary containing additional key-value pairs.
        /// </summary>
        [Property("metadata")]
        public Dictionary<string, object> Metadata { get; set; }

        /// <summary>
        /// Gets or sets the address associated with the entity.
        /// </summary>
        [Property("address")]
        public Address Address { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the entity is active.
        /// </summary>
        [Property("is_active")]
        public bool IsActive { get; set; }

        /// <summary>
        /// Gets or sets an optional integer value.
        /// </summary>
        [Property("nullable_int")]
        public int? NullableInt { get; set; }

        /// <summary>
        /// Gets or sets the price associated with the entity.
        /// </summary>
        [Property("price")]
        public decimal Price { get; set; }

        /// <summary>
        /// Returns a string representation of the complex entity including key information.
        /// </summary>
        /// <returns>A formatted string containing the entity's details.</returns>
        public override string ToString()
        {
            return $"ComplexEntity: Id={Id}, Name={Name}, CreatedDate={CreatedDate:yyyy-MM-dd HH:mm:ss}, " +
                   $"UniqueId={UniqueId}, Status={Status}, TagCount={Tags?.Length ?? 0}";
        }
    }
}