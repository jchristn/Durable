namespace Durable
{
    using System.Diagnostics;
    using System.Reflection;

    /// <summary>
    /// OpenTelemetry-compatible tracing for Durable. Subscribe with
    /// <c>tracerProviderBuilder.AddSource(DurableDiagnostics.ActivitySourceName)</c>.
    /// Activities follow the OpenTelemetry database semantic conventions (<c>db.system</c>, <c>db.query.text</c>,
    /// <c>db.operation.name</c>, <c>db.collection.name</c>, <c>db.response.returned_rows</c>).
    /// Thread safety: all members are thread-safe.
    /// </summary>
    public static class DurableDiagnostics
    {
        /// <summary>
        /// The activity source name: "Durable".
        /// </summary>
        public const string ActivitySourceName = "Durable";

        /// <summary>
        /// The shared activity source. Activities are only created when a listener is attached.
        /// </summary>
        public static readonly ActivitySource ActivitySource = new ActivitySource(
            ActivitySourceName,
            typeof(DurableDiagnostics).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion);
    }
}
