namespace Durable
{
    public class CteDefinition
    {
        public string Name { get; set; } = string.Empty;
        public string Query { get; set; } = string.Empty;
        public bool IsRecursive { get; set; }
        public string AnchorQuery { get; set; } = string.Empty;
        public string RecursiveQuery { get; set; } = string.Empty;

        public CteDefinition(string name, string query)
        {
            Name = name;
            Query = query;
            IsRecursive = false;
        }

        public CteDefinition(string name, string anchorQuery, string recursiveQuery)
        {
            Name = name;
            AnchorQuery = anchorQuery;
            RecursiveQuery = recursiveQuery;
            IsRecursive = true;
        }
    }
}