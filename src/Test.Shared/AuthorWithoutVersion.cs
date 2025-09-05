using Durable;

namespace Test.Sqlite
{
    [Entity("authors")]
    public class AuthorWithoutVersion
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }
        
        [Property("name", Flags.String)]
        public string Name { get; set; }
        
        [Property("company_id")]
        public int? CompanyId { get; set; }
    }
}