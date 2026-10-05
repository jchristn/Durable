namespace Durable.Conformance
{
    using Durable;

    /// <summary>
    /// Document with a reference navigation to a soft-deletable <see cref="CfFolder"/>. Storage: <c>cf_documents</c>.
    /// </summary>
    [Entity("cf_documents")]
    public class CfDocument
    {
        /// <summary>Gets or sets the generated identity key.</summary>
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        /// <summary>Gets or sets the title. Never null.</summary>
        [Property("title", Flags.String, 64)]
        public string Title { get; set; } = string.Empty;

        /// <summary>Gets or sets the folder key.</summary>
        [Property("folder_id")]
        [ForeignKey(typeof(CfFolder), "Id")]
        public int FolderId { get; set; }

        /// <summary>Gets or sets the folder; null when not loaded or when the folder is soft-deleted.</summary>
        [NavigationProperty("FolderId")]
        public CfFolder? Folder { get; set; }
    }
}
