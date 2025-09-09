namespace Durable
{
    public class SetOperation<T> where T : class, new()
    {
        public SetOperationType Type { get; set; }
        public IQueryBuilder<T> OtherQuery { get; set; }

        public SetOperation(SetOperationType type, IQueryBuilder<T> otherQuery)
        {
            Type = type;
            OtherQuery = otherQuery;
        }
    }
}