namespace Test.Sqlite
{
    public class TestEntityWithReadOnlyProperty
    {
        public int Id { get; set; }
        public string ReadOnlyProperty { get; } = "Cannot be written";
    }
}