namespace Test.Shared
{
    /// <summary>
    /// Represents a physical address with street, city, and ZIP code information.
    /// </summary>
    public class Address
    {
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.

        #region Public-Members

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

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the <see cref="Address"/> class.
        /// </summary>
        public Address()
        {
        }

        #endregion

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider adding the 'required' modifier or declaring as nullable.
    }
}