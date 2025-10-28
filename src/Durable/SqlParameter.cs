namespace Durable
{
    /// <summary>
    /// Represents a SQL parameter with a name and value.
    /// </summary>
    public class SqlParameter
    {
        /// <summary>
        /// The parameter name (including @ prefix if applicable).
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The parameter value.
        /// </summary>
        public object? Value { get; set; }

        /// <summary>
        /// Creates a new SQL parameter.
        /// </summary>
        public SqlParameter(string name, object? value)
        {
            Name = name;
            Value = value;
        }
    }
}
