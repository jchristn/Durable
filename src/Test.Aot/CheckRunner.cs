namespace Test.Aot
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Records PASS/FAIL per named check and prints each result.
    /// </summary>
    internal sealed class CheckRunner
    {
        public int Passed { get; private set; }

        public int Failed { get; private set; }

        public void Check(string name, Func<bool> test)
        {
            try
            {
                Report(name, test(), null);
            }
            catch (Exception ex)
            {
                Report(name, false, ex);
            }
        }

        public async Task CheckAsync(string name, Func<Task<bool>> test)
        {
            try
            {
                Report(name, await test().ConfigureAwait(false), null);
            }
            catch (Exception ex)
            {
                Report(name, false, ex);
            }
        }

        public void Fail(string name, Exception ex)
        {
            Report(name, false, ex);
        }

        private void Report(string name, bool passed, Exception? ex)
        {
            if (passed)
            {
                Passed++;
                Console.WriteLine("PASS  " + name);
                return;
            }

            Failed++;
            Console.WriteLine("FAIL  " + name + (ex == null ? string.Empty : " -> " + ex.GetType().Name + ": " + ex.Message));
            if (ex != null) Console.WriteLine(ex.StackTrace);
        }
    }
}
