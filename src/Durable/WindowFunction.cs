namespace Durable
{
    using System.Collections.Generic;

    public class WindowFunction
    {
        public string FunctionName { get; set; } = string.Empty;
        public string Column { get; set; } = string.Empty;
        public string Alias { get; set; } = string.Empty;
        public List<string> PartitionByColumns { get; set; }
        public List<WindowOrderByClause> OrderByColumns { get; set; }
        public WindowFrame Frame { get; set; } = new WindowFrame();
        public Dictionary<string, object> Parameters { get; set; }

        public WindowFunction()
        {
            PartitionByColumns = new List<string>();
            OrderByColumns = new List<WindowOrderByClause>();
            Parameters = new Dictionary<string, object>();
        }
    }
}