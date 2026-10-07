namespace Test.Shared
{
    using System;

    /// <summary>
    /// Mapping-source test document with a version column, soft delete, a generated public id and a unique composite index.
    /// Carries no Durable attributes: it is mapped through <see cref="MapAttributeMappingSource"/>.
    /// </summary>
    [MapTable("ms_documents")]
    [MapCompositeIndex("idx_ms_documents_owner_title", "owner", "title", Unique = true)]
    public class MsDocument
    {
        /// <summary>Gets or sets the key.</summary>
        [MapColumn("doc_id", Key = true, Identity = true)]
        public int Id { get; set; }

        /// <summary>Gets or sets the public id, generated on insert when empty.</summary>
        [MapColumn("public_id")]
        [MapNewGuid]
        public Guid PublicId { get; set; }

        /// <summary>Gets or sets the owner.</summary>
        [MapColumn("owner", Length = 32)]
        public string Owner { get; set; } = string.Empty;

        /// <summary>Gets or sets the title.</summary>
        [MapColumn("title", Length = 64)]
        public string Title { get; set; } = string.Empty;

        /// <summary>Gets or sets the revision (optimistic concurrency).</summary>
        [MapColumn("revision")]
        [MapVersion]
        public int Revision { get; set; }

        /// <summary>Gets or sets whether the document is deleted.</summary>
        [MapColumn("is_deleted")]
        [MapSoftDelete]
        public bool Deleted { get; set; }
    }
}
