namespace Test.Shared
{
    /// <summary>
    /// Represents a physical address with street, city, and ZIP code information.
    /// </summary>
    public class Address
    {
        /// <summary>
        /// Gets or sets the street address.
        /// </summary>
        public string Street { get; set; }
        
        /// <summary>
        /// Gets or sets the city name.
        /// </summary>
        public string City { get; set; }
        
        /// <summary>
        /// Gets or sets the ZIP/postal code.
        /// </summary>
        public string ZipCode { get; set; }
    }
}