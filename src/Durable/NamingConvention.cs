namespace Durable
{
    /// <summary>
    /// Naming convention applied to table and column names for entities mapped by convention.
    /// </summary>
    public enum NamingConvention
    {
        /// <summary>
        /// Use the class or property name unchanged.
        /// </summary>
        AsIs = 0,

        /// <summary>
        /// Convert PascalCase names to snake_case (for example, FirstName becomes first_name).
        /// </summary>
        SnakeCase = 1,

        /// <summary>
        /// Lower-case the name without separators (for example, FirstName becomes firstname).
        /// </summary>
        Lowercase = 2
    }
}
