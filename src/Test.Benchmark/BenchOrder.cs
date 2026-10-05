namespace Test.Benchmark
{
    using System;
    using Durable;

    /// <summary>
    /// Benchmark entity with a realistic mix of column types.
    /// </summary>
    [Entity("bench_orders")]
    public class BenchOrder
    {
        /// <summary>Primary key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Customer number (100 distinct values, 100 rows each).</summary>
        [Property("customer_id")]
        public long CustomerId { get; set; }

        /// <summary>Customer name.</summary>
        [Property("customer_name", Flags.String, 100)]
        public string CustomerName { get; set; } = string.Empty;

        /// <summary>Optional notes; null on every other row.</summary>
        [Property("notes", Flags.String, 200)]
        public string? Notes { get; set; }

        /// <summary>Creation time.</summary>
        [Property("created_utc")]
        public DateTime CreatedUtc { get; set; }

        /// <summary>Order total.</summary>
        [Property("total")]
        public decimal Total { get; set; }

        /// <summary>Whether the order is paid.</summary>
        [Property("is_paid")]
        public bool IsPaid { get; set; }

        /// <summary>Status, stored as a string.</summary>
        [Property("status", Flags.String, 20)]
        public OrderStatus Status { get; set; }

        /// <summary>External reference.</summary>
        [Property("reference")]
        public Guid Reference { get; set; }

        /// <summary>Optional priority; null on every third row.</summary>
        [Property("priority")]
        public int? Priority { get; set; }
    }
}
