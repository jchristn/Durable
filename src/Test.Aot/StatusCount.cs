namespace Test.Aot
{
    /// <summary>
    /// GroupBy projection DTO.
    /// </summary>
    public class StatusCount
    {
        public AuthorStatus Status { get; set; }

        public int Count { get; set; }

        public int TotalRating { get; set; }
    }
}
