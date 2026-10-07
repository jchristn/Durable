namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Durable.Sql;

    /// <summary>
    /// Gates a shared test case on a dialect feature. The case is built as skipped, with a reason naming the feature and the
    /// dialect, when the configured provider's dialect does not meet every requirement on the method; it is never run and
    /// reported as passed. Apply it once per requirement.
    /// Thread safety: immutable.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
    public sealed class RequiresDialectAttribute : Attribute
    {
        #region Public-Members

        /// <summary>
        /// Gets the required dialect feature.
        /// </summary>
        public DialectRequirement Requirement { get; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes the attribute.
        /// </summary>
        /// <param name="requirement">The dialect feature the case needs.</param>
        public RequiresDialectAttribute(DialectRequirement requirement)
        {
            Requirement = requirement;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Returns the skip reason for a test method under a dialect, or null when the dialect meets every requirement on it.
        /// </summary>
        /// <param name="method">The test method. Must not be null.</param>
        /// <param name="dialect">The configured dialect. Must not be null.</param>
        /// <returns>The reason, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        public static string? SkipReason(MethodInfo method, ISqlDialect dialect)
        {
            ArgumentNullException.ThrowIfNull(method);
            ArgumentNullException.ThrowIfNull(dialect);
            List<string> unmet = new List<string>();
            foreach (RequiresDialectAttribute attribute in method.GetCustomAttributes<RequiresDialectAttribute>())
            {
                if (!IsMet(attribute.Requirement, dialect)) unmet.Add(Describe(attribute.Requirement));
            }

            if (unmet.Count == 0) return null;
            return "Requires a dialect where " + string.Join(" and ", unmet) + "; " + dialect.RepositoryType.DisplayName + " does not qualify.";
        }

        /// <summary>
        /// Returns whether a dialect meets a requirement.
        /// </summary>
        /// <param name="requirement">The requirement.</param>
        /// <param name="dialect">The dialect. Must not be null.</param>
        /// <returns>True when met.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="dialect"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown for an unknown requirement.</exception>
        public static bool IsMet(DialectRequirement requirement, ISqlDialect dialect)
        {
            ArgumentNullException.ThrowIfNull(dialect);
            switch (requirement)
            {
                case DialectRequirement.Savepoints: return dialect.SupportsSavepoints;
                case DialectRequirement.NoSavepoints: return !dialect.SupportsSavepoints;
                case DialectRequirement.StoredProcedures: return dialect.SupportsStoredProcedures;
                case DialectRequirement.NoStoredProcedures: return !dialect.SupportsStoredProcedures;
                case DialectRequirement.EmptyStrings: return !dialect.TreatsEmptyStringAsNull;
                default: throw new ArgumentOutOfRangeException(nameof(requirement), requirement, "Unknown dialect requirement.");
            }
        }

        #endregion

        #region Private-Methods

        private static string Describe(DialectRequirement requirement)
        {
            switch (requirement)
            {
                case DialectRequirement.Savepoints: return "savepoints are supported";
                case DialectRequirement.NoSavepoints: return "savepoints are not supported";
                case DialectRequirement.StoredProcedures: return "stored procedures are supported";
                case DialectRequirement.NoStoredProcedures: return "stored procedures are not supported";
                case DialectRequirement.EmptyStrings: return "empty strings are distinct from NULL";
                default: return requirement.ToString();
            }
        }

        #endregion
    }
}
