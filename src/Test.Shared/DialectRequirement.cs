namespace Test.Shared
{
    /// <summary>
    /// A dialect feature a shared test case needs (or, for the <c>No*</c> values, must lack). Used with
    /// <see cref="RequiresDialectAttribute"/>: a case whose requirement the configured dialect does not meet is reported
    /// as skipped with the reason, never as a pass.
    /// </summary>
    public enum DialectRequirement
    {
        /// <summary>The dialect supports savepoints (<c>ISqlDialect.SupportsSavepoints</c>).</summary>
        Savepoints,

        /// <summary>The dialect does not support savepoints (the case checks the NotSupportedException path).</summary>
        NoSavepoints,

        /// <summary>The dialect supports stored procedures (<c>ISqlDialect.SupportsStoredProcedures</c>).</summary>
        StoredProcedures,

        /// <summary>The dialect does not support stored procedures (the case checks the NotSupportedException path).</summary>
        NoStoredProcedures,

        /// <summary>Empty strings are kept distinct from NULL (<c>ISqlDialect.TreatsEmptyStringAsNull</c> is false).</summary>
        EmptyStrings
    }
}
