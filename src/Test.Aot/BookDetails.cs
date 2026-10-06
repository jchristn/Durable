namespace Test.Aot
{
    using System.Collections.Generic;

    /// <summary>
    /// Complex value stored in a JSON column.
    /// </summary>
    public class BookDetails
    {
        public int Pages { get; set; }

        public string? Publisher { get; set; }

        public Dictionary<string, string> Extra { get; set; } = new Dictionary<string, string>();
    }
}
