namespace Test.Aot
{
    using System.Collections.Generic;
    using System.Text.Json.Serialization;

    /// <summary>
    /// Source-generated JSON metadata for every JSON column type (camelCase, matching Durable's default JSON options).
    /// </summary>
    [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
    [JsonSerializable(typeof(BookDetails))]
    [JsonSerializable(typeof(List<string>))]
    internal partial class AotJsonContext : JsonSerializerContext
    {
    }
}
