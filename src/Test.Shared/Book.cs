namespace Test.Shared
{
    using Durable;
    using System.ComponentModel.DataAnnotations;
    
    [Entity("books")]
    public class Book
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("title", Flags.String, 200)]
        [Required(ErrorMessage = "Book title is required")]
        [StringLength(200, MinimumLength = 1, ErrorMessage = "Book title must be between 1 and 200 characters")]
        public string Title { get; set; }

        [Property("author_id")]
        [ForeignKey(typeof(Author), "Id")]
        [Required(ErrorMessage = "Author ID is required")]
        [Range(1, int.MaxValue, ErrorMessage = "Author ID must be a positive number")]
        public int AuthorId { get; set; }

        [NavigationProperty("AuthorId")]
        public Author Author { get; set; }

        [Property("publisher_id")]
        [ForeignKey(typeof(Company), "Id")]
        [Range(1, int.MaxValue, ErrorMessage = "Publisher ID must be a positive number when specified")]
        public int? PublisherId { get; set; }

        [NavigationProperty("PublisherId")]
        public Company Publisher { get; set; }

        public override string ToString()
        {
            return $"Book: Id={Id}, Title={Title}, AuthorId={AuthorId}, Author={Author?.Name ?? "null"}, Publisher={Publisher?.Name ?? "null"}";
        }
    }
}