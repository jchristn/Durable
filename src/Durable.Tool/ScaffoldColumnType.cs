namespace Durable.Tool
{
    /// <summary>
    /// The CLR type chosen for a scaffolded column.
    /// </summary>
    internal sealed class ScaffoldColumnType
    {
        /// <summary>Gets the C# type name without nullability, for example "int" or "byte[]".</summary>
        public string TypeName { get; }

        /// <summary>Gets whether the type is a reference type (string, byte[]).</summary>
        public bool IsReferenceType { get; }

        /// <summary>Gets whether the column holds text (gets Flags.String and a MaxLength).</summary>
        public bool IsText { get; }

        /// <summary>Gets the initializer for a non-nullable reference type, for example "string.Empty"; null otherwise.</summary>
        public string? Initializer { get; }

        /// <summary>Gets a note emitted as a comment when the database type has no exact CLR equivalent; null otherwise.</summary>
        public string? Note { get; }

        /// <summary>
        /// Instantiates a column type.
        /// </summary>
        /// <param name="typeName">C# type name. Must not be null.</param>
        /// <param name="isReferenceType">Whether the type is a reference type.</param>
        /// <param name="isText">Whether the column holds text.</param>
        /// <param name="initializer">Initializer for non-nullable reference types, or null.</param>
        /// <param name="note">Mapping note, or null.</param>
        public ScaffoldColumnType(string typeName, bool isReferenceType, bool isText, string? initializer, string? note = null)
        {
            TypeName = typeName;
            IsReferenceType = isReferenceType;
            IsText = isText;
            Initializer = initializer;
            Note = note;
        }
    }
}
