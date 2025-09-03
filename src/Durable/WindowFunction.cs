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

    public class WindowFrame
    {
        public WindowFrameType Type { get; set; }
        public WindowFrameBound StartBound { get; set; } = new WindowFrameBound();
        public WindowFrameBound EndBound { get; set; } = new WindowFrameBound();
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
        public string Column { get; set; } = string.Empty;
        public bool Ascending { get; set; }
    }
}