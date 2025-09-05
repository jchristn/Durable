using Durable;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Test.Shared
{
    [Entity("companies")]
    public class Company
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        [Required(ErrorMessage = "Company name is required")]
        [StringLength(100, MinimumLength = 1, ErrorMessage = "Company name must be between 1 and 100 characters")]
        public string Name { get; set; }

        [Property("industry", Flags.String, 50)]
        [StringLength(50, ErrorMessage = "Industry name cannot exceed 50 characters")]
        public string Industry { get; set; }

        [InverseNavigationProperty("CompanyId")]
        public List<Author> Employees { get; set; } = new List<Author>();

        [InverseNavigationProperty("PublisherId")]
        public List<Book> PublishedBooks { get; set; } = new List<Book>();

        public override string ToString()
        {
            return $"Company: Id={Id}, Name={Name}, Industry={Industry}, Employees={Employees?.Count ?? 0}, Published Books={PublishedBooks?.Count ?? 0}";
        }
    }
}