namespace Durable.Conformance
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Xunit;

    /// <summary>
    /// Assertion helpers with failure messages that name the operation and the backend.
    /// </summary>
    internal static class ConformanceAssert
    {
        internal static void NameSet(IEnumerable<string?> actual, IEnumerable<string?> expected, string context)
        {
            List<string?> actualSorted = actual.OrderBy(n => n, StringComparer.Ordinal).ToList();
            List<string?> expectedSorted = expected.OrderBy(n => n, StringComparer.Ordinal).ToList();
            Assert.True(actualSorted.SequenceEqual(expectedSorted, StringComparer.Ordinal),
                context + ": expected [" + Describe(expectedSorted) + "] but got [" + Describe(actualSorted) + "]");
        }

        internal static void Sequence(IEnumerable<string?> actual, IEnumerable<string?> expected, string context)
        {
            List<string?> actualList = actual.ToList();
            List<string?> expectedList = expected.ToList();
            Assert.True(actualList.SequenceEqual(expectedList, StringComparer.Ordinal),
                context + ": expected sequence [" + Describe(expectedList) + "] but got [" + Describe(actualList) + "]");
        }

        internal static void NotSupported(Action action, string operation, string capability)
        {
            ArgumentNullException.ThrowIfNull(action);
            Exception? caught = null;
            try
            {
                action();
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            Check(caught, operation, capability);
        }

        internal static async Task NotSupportedAsync(Func<Task> action, string operation, string capability)
        {
            ArgumentNullException.ThrowIfNull(action);
            Exception? caught = null;
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            Check(caught, operation, capability);
        }

        internal static void Throws<TException>(Action action, string operation) where TException : Exception
        {
            Exception? caught = null;
            try
            {
                action();
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            Assert.True(caught is TException,
                operation + " should throw " + typeof(TException).Name + " but "
                + (caught == null ? "did not throw" : "threw " + caught.GetType().Name + ": " + caught.Message));
        }

        internal static async Task ThrowsAsync<TException>(Func<Task> action, string operation) where TException : Exception
        {
            Exception? caught = null;
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            Assert.True(caught is TException,
                operation + " should throw " + typeof(TException).Name + " but "
                + (caught == null ? "did not throw" : "threw " + caught.GetType().Name + ": " + caught.Message));
        }

        internal static void SameInstant(DateTime expected, DateTime actual, string context)
        {
            Assert.True(expected.Ticks == actual.Ticks,
                context + ": expected " + expected.ToString("O") + " but got " + actual.ToString("O") + " (kind is ignored)");
        }

        internal static string Describe(IEnumerable<string?> names)
        {
            return string.Join(", ", names.Select(n => n == null ? "null" : "\"" + n.Replace("\n", "\\n", StringComparison.Ordinal) + "\""));
        }

        private static void Check(Exception? caught, string operation, string capability)
        {
            if (caught == null)
                Assert.Fail(operation + " must throw NotSupportedException at the call site because the target does not support " + capability + ", but it did not throw.");
            if (caught is not NotSupportedException)
                Assert.Fail(operation + " must throw NotSupportedException because the target does not support " + capability + ", but it threw "
                    + caught!.GetType().Name + ": " + caught.Message);
        }
    }
}
