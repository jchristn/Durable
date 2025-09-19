namespace Test.Sqlite
{
    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
        public bool IsActive { get; set; }
        public string? Description { get; set; }
    }
}