using Durable;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Test.Shared
{
    [Entity("authors")]
    public class Author
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        [Required(ErrorMessage = "Author name is required")]
        [StringLength(100, MinimumLength = 1, ErrorMessage = "Author name must be between 1 and 100 characters")]
        public string Name { get; set; }

        [Property("company_id")]
        [ForeignKey(typeof(Company), "Id")]
        [Range(1, int.MaxValue, ErrorMessage = "Company ID must be a positive number when specified")]
        public int? CompanyId { get; set; }

        [NavigationProperty("CompanyId")]
        public Company Company { get; set; }

        [InverseNavigationProperty("AuthorId")]
        public List<Book> Books { get; set; } = new List<Book>();

        [ManyToManyNavigationProperty(typeof(AuthorCategory), "AuthorId", "CategoryId")]
        public List<Category> Categories { get; set; } = new List<Category>();

        public override string ToString()
        {
            return $"Author: Id={Id}, Name={Name}, CompanyId={CompanyId}, Company={Company?.Name ?? "null"}, Books Count={Books?.Count ?? 0}, Categories Count={Categories?.Count ?? 0}";
        }
    }
}