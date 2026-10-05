namespace Durable.Conformance
{
    /// <summary>
    /// Item status. Stored by name (the default enum storage) in <see cref="CfItem.Status"/>.
    /// </summary>
    public enum CfStatus
    {
        /// <summary>Draft.</summary>
        Draft = 0,

        /// <summary>Active.</summary>
        Active = 1,

        /// <summary>Suspended.</summary>
        Suspended = 2,

        /// <summary>Closed.</summary>
        Closed = 3
    }
}
