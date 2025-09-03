namespace Durable
{
    using System.Collections.Generic;

    public class WindowFunction
    {
        public string FunctionName { get; set; }
        public string Column { get; set; }
        public string Alias { get; set; }
        public List<string> PartitionByColumns { get; set; }
        public List<WindowOrderByClause> OrderByColumns { get; set; }
        public WindowFrame Frame { get; set; }
        public Dictionary<string, object> Parameters { get; set; }

        public WindowFunction()
        {
            PartitionByColumns = new List<string>();
            OrderByColumns = new List<WindowOrderByClause>();
            Parameters = new Dictionary<string, object>();
        }
    }

    public class WindowFrame
    {
        public WindowFrameType Type { get; set; }
        public WindowFrameBound StartBound { get; set; }
        public WindowFrameBound EndBound { get; set; }
    }

    public enum WindowFrameType
    {
        Rows,
        Range,
        Groups
    }

    public class WindowFrameBound
    {
        public WindowFrameBoundType Type { get; set; }
        public int? Offset { get; set; }
    }

    public enum WindowFrameBoundType
    {
        UnboundedPreceding,
        UnboundedFollowing,
        CurrentRow,
        Preceding,
        Following
    }

    public class WindowOrderByClause
    {
        public string Column { get; set; }
        public bool Ascending { get; set; }
    }
}