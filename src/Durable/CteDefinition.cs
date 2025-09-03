namespace Durable
{
    public class CteDefinition
    {
        public string Name { get; set; }
        public string Query { get; set; }
        public bool IsRecursive { get; set; }
        public string AnchorQuery { get; set; }
        public string RecursiveQuery { get; set; }

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