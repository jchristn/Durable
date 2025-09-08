namespace Durable
{
    public class WhenClause
    {
        public string Condition { get; set; } = string.Empty;
        public string Result { get; set; } = string.Empty;

        public WhenClause(string condition, string result)
        {
            Condition = condition;
            Result = result;
        }
    }
}