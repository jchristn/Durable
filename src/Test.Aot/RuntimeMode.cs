namespace Test.Aot
{
    using System.Runtime;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// Describes how the app is running. A project with PublishAot=true also disables dynamic code under
    /// <c>dotnet run</c> (to surface AOT behavior early), so "no dynamic code" alone does not mean Native AOT.
    /// </summary>
    internal static class RuntimeMode
    {
        public static bool IsNativeAot => JitInfo.GetCompiledMethodCount(false) == 0;

        public static string Describe()
        {
            if (IsNativeAot) return "Native AOT";
            return RuntimeFeature.IsDynamicCodeSupported ? "JIT" : "JIT (dynamic code disabled)";
        }
    }
}
