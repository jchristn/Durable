namespace Test.Sqlite
{
    using Durable;

    [Entity("authors")]
    public class AuthorWithVersion
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }
        
        [Property("name", Flags.String)]
        public string Name { get; set; }
        
        [Property("company_id")]
        public int? CompanyId { get; set; }
        
        [Property("version")]
        [VersionColumn(VersionColumnType.Integer)]
        public int Version { get; set; }
    }
}