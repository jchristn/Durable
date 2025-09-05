using Durable;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Test.Shared
{
    [Entity("categories")]
    public class Category
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("name", Flags.String, 100)]
        [Required(ErrorMessage = "Category name is required")]
        [StringLength(100, MinimumLength = 1, ErrorMessage = "Category name must be between 1 and 100 characters")]
        public string Name { get; set; }

        [Property("description", Flags.String, 255)]
        [StringLength(255, ErrorMessage = "Description cannot exceed 255 characters")]
        public string Description { get; set; }

        [ManyToManyNavigationProperty(typeof(AuthorCategory), "CategoryId", "AuthorId")]
        public List<Author> Authors { get; set; } = new List<Author>();

        public override string ToString()
        {
            return $"Category: Id={Id}, Name={Name}, Description={Description}, Authors Count={Authors?.Count ?? 0}";
        }
    }
}