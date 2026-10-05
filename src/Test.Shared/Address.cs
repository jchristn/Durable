namespace Test.Shared
{
    /// <summary>
    /// Represents a physical address with street, city, and ZIP code information.
    /// </summary>
    public class Address
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the street address.
        /// </summary>
        public string Street { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the city name.
        /// </summary>
        public string City { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets or sets the ZIP/postal code.
        /// </summary>
        public string ZipCode { get; set; } = string.Empty;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="Address"/> class.
        /// </summary>
        public Address()
        {
        }

        #endregion
    }
}