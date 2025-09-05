using Durable;

namespace Test.Shared
{
    [Entity("author_categories")]
    public class AuthorCategory
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("author_id")]
        [ForeignKey(typeof(Author), "Id")]
        public int AuthorId { get; set; }

        [Property("category_id")]
        [ForeignKey(typeof(Category), "Id")]
        public int CategoryId { get; set; }

        [NavigationProperty("AuthorId")]
        public Author Author { get; set; }

        [NavigationProperty("CategoryId")]
        public Category Category { get; set; }

        public override string ToString()
        {
            return $"AuthorCategory: Id={Id}, AuthorId={AuthorId}, CategoryId={CategoryId}";
        }
    }
}