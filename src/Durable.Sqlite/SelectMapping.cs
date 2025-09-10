namespace Durable.Sqlite
{
    using System.Reflection;

    public class SelectMapping
    {
        public string ColumnName { get; set; }
        public string Alias { get; set; }
        public PropertyInfo SourceProperty { get; set; }
        public PropertyInfo TargetProperty { get; set; }
    }
}