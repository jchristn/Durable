namespace Test.Shared
{
    using System;
    using Durable;

    /// <summary>
    /// LiteDB test entity with an auto-increment Guid key, which the LiteDB backend does not support.
    /// </summary>
    [Entity("ldb_guid_identities")]
    public class LiteDbGuidIdentity
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public Guid Id { get; set; }
    }
}
