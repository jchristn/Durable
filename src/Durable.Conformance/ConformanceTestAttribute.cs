namespace Durable.Conformance
{
    using System;
    using Durable;

    /// <summary>
    /// Marks a public instance method of a conformance suite as a test case. The method takes no parameters and returns
    /// <c>void</c> or <see cref="System.Threading.Tasks.Task"/>. When <see cref="Requires"/> names capabilities the target
    /// lacks, the case is reported as skipped with a reason naming them (the capability suite verifies the corresponding
    /// operations throw <see cref="NotSupportedException"/>), so nothing is skipped silently.
    /// Thread safety: attributes are immutable after construction.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
    public sealed class ConformanceTestAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets the capabilities the case needs. Default: <see cref="RepositoryCapabilities.None"/> (always runs).
        /// </summary>
        public RepositoryCapabilities Requires { get; set; } = RepositoryCapabilities.None;

        /// <summary>
        /// Gets or sets an optional one-line description shown in the case's display name. Default: null (the method name
        /// alone is used).
        /// </summary>
        public string? Description { get; set; } = null;
    }
}
