using System;
using System.Collections.Generic;
using Durable;

namespace Test.Shared
{
    public enum Status
    {
        Active,
        Inactive,
        Pending
    }

    public class Address
    {
        public string Street { get; set; }
        public string City { get; set; }
        public string ZipCode { get; set; }
    }

    [Entity("complex_entities")]
    public class ComplexEntity
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        public string Name { get; set; }

        [Property("created_date")]
        public DateTime CreatedDate { get; set; }

        [Property("updated_date")]
        public DateTimeOffset? UpdatedDate { get; set; }

        [Property("unique_id")]
        public Guid UniqueId { get; set; }

        [Property("duration")]
        public TimeSpan Duration { get; set; }

        [Property("status")]
        public Status Status { get; set; }

        [Property("status_int")]
        public Status StatusAsInt { get; set; }

        [Property("tags")]
        public string[] Tags { get; set; }

        [Property("scores")]
        public List<int> Scores { get; set; }

        [Property("metadata")]
        public Dictionary<string, object> Metadata { get; set; }

        [Property("address")]
        public Address Address { get; set; }

        [Property("is_active")]
        public bool IsActive { get; set; }

        [Property("nullable_int")]
        public int? NullableInt { get; set; }

        [Property("price")]
        public decimal Price { get; set; }

        public override string ToString()
        {
            return $"ComplexEntity: Id={Id}, Name={Name}, CreatedDate={CreatedDate:yyyy-MM-dd HH:mm:ss}, " +
                   $"UniqueId={UniqueId}, Status={Status}, TagCount={Tags?.Length ?? 0}";
        }
    }
}