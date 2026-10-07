namespace Test.Shared
{
    /// <summary>
    /// A column name and data type read from Oracle's USER_TAB_COLUMNS by the Oracle provider suite.
    /// </summary>
    public class OraColumnType
    {
        /// <summary>
        /// Gets or sets the column name as stored. Never null.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the data type as reported by the data dictionary. Never null.
        /// </summary>
        public string DataType { get; set; } = string.Empty;
    }
}
