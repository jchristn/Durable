namespace Durable
{
    /// <summary>
    /// Columns resolved for a navigation; internal holder used by <see cref="NavigationMetadata"/>.
    /// </summary>
    internal sealed class ResolvedNavigation
    {
        public ColumnMetadata LocalColumn { get; set; } = null!;

        public ColumnMetadata RemoteColumn { get; set; } = null!;

        public ColumnMetadata? JunctionLocalColumn { get; set; }

        public ColumnMetadata? JunctionRemoteColumn { get; set; }
    }
}
