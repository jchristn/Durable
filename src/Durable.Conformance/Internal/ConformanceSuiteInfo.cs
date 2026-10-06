namespace Durable.Conformance
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Registration of one built-in suite: its id suffix, display name and class.
    /// </summary>
    internal sealed class ConformanceSuiteInfo
    {
        internal ConformanceSuiteInfo(string id, string displayName, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods)] Type suiteType)
        {
            Id = id;
            DisplayName = displayName;
            SuiteType = suiteType;
        }

        internal string Id { get; }

        internal string DisplayName { get; }

        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods)]
        internal Type SuiteType { get; }
    }
}
