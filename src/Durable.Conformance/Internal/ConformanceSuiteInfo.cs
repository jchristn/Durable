namespace Durable.Conformance
{
    using System;

    /// <summary>
    /// Registration of one built-in suite: its id suffix, display name and class.
    /// </summary>
    internal sealed class ConformanceSuiteInfo
    {
        internal ConformanceSuiteInfo(string id, string displayName, Type suiteType)
        {
            Id = id;
            DisplayName = displayName;
            SuiteType = suiteType;
        }

        internal string Id { get; }

        internal string DisplayName { get; }

        internal Type SuiteType { get; }
    }
}
