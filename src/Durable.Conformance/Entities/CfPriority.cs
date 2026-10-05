namespace Durable.Conformance
{
    /// <summary>
    /// Item priority. Stored as its integer value (<see cref="Flags.Integer"/>) in <see cref="CfItem.Priority"/>.
    /// </summary>
    public enum CfPriority
    {
        /// <summary>Low (1).</summary>
        Low = 1,

        /// <summary>Medium (2).</summary>
        Medium = 2,

        /// <summary>High (3).</summary>
        High = 3,

        /// <summary>Critical (4).</summary>
        Critical = 4
    }
}
