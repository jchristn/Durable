namespace Durable
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using System.Text.Json.Serialization.Metadata;

    /// <summary>
    /// JSON serialization for JSON columns (collections, complex objects and <see cref="Flags.Json"/> properties), shared by
    /// every backend. Values are serialized through the <see cref="JsonTypeInfo"/> that the options' resolver supplies, so
    /// the same code serves both runtimes:
    /// <list type="bullet">
    /// <item>On the JIT (reflection enabled), options without a <see cref="JsonSerializerOptions.TypeInfoResolver"/> use the
    /// reflection-based resolver, exactly like <see cref="JsonSerializer"/>'s <see cref="Type"/>-based overloads.</item>
    /// <item>Under trimming or Native AOT, reflection-based serialization is disabled; supply options whose
    /// <see cref="JsonSerializerOptions.TypeInfoResolver"/> is a source-generated <see cref="JsonSerializerContext"/> that
    /// declares <c>[JsonSerializable(typeof(X))]</c> for every JSON column type (see <see cref="CreateOptions"/>).</item>
    /// </list>
    /// Thread safety: all members are thread-safe; options become read-only on first use.
    /// </summary>
    public static class DurableJson
    {
        /// <summary>
        /// Creates options with Durable's JSON column defaults (camelCase property names, compact output) and, optionally,
        /// a type-info resolver such as a source-generated <see cref="JsonSerializerContext"/>. Pass the result to a
        /// backend: <c>new SqliteDataTypeConverter(options)</c> via <c>SqlRepositoryOptions.DataTypeConverter</c> (or the
        /// dialect constructor), the <c>JsonOptions</c> of <c>InMemoryRepositorySettings</c>, <c>LiteDbRepositorySettings</c> or <c>LiteGraphRepositorySettings</c>.
        /// For on-disk JSON identical to the JIT default, declare the context with
        /// <c>[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]</c>.
        /// </summary>
        /// <param name="resolver">Type-info resolver (for example <c>MyJsonContext.Default</c>); null keeps the default
        /// (reflection on the JIT; unavailable under Native AOT).</param>
        /// <returns>New, still mutable options.</returns>
        public static JsonSerializerOptions CreateOptions(IJsonTypeInfoResolver? resolver = null)
        {
            JsonSerializerOptions options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };
            if (resolver != null) options.TypeInfoResolver = resolver;
            return options;
        }

        /// <summary>
        /// Serializes a value as JSON text.
        /// </summary>
        /// <param name="value">Value; may be null (serialized as <c>null</c>).</param>
        /// <param name="type">Type whose contract is used. Must not be null.</param>
        /// <param name="options">Options. Must not be null.</param>
        /// <returns>The JSON text.</returns>
        /// <exception cref="ArgumentNullException">Thrown when type or options is null.</exception>
        /// <exception cref="NotSupportedException">Thrown when no JSON metadata is available for the type (for example under
        /// Native AOT without a resolver that declares it).</exception>
        public static string Serialize(object? value, Type type, JsonSerializerOptions options)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(options);
            return JsonSerializer.Serialize(value, GetTypeInfo(type, options));
        }

        /// <summary>
        /// Deserializes JSON text.
        /// </summary>
        /// <param name="json">JSON text. Must not be null.</param>
        /// <param name="type">Target type. Must not be null.</param>
        /// <param name="options">Options. Must not be null.</param>
        /// <returns>The value; may be null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null.</exception>
        /// <exception cref="JsonException">Thrown when the JSON is invalid for the type.</exception>
        /// <exception cref="NotSupportedException">Thrown when no JSON metadata is available for the type.</exception>
        public static object? Deserialize(string json, Type type, JsonSerializerOptions options)
        {
            ArgumentNullException.ThrowIfNull(json);
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(options);
            return JsonSerializer.Deserialize(json, GetTypeInfo(type, options));
        }

        /// <summary>
        /// Deserializes UTF-8 JSON.
        /// </summary>
        /// <param name="utf8Json">UTF-8 encoded JSON.</param>
        /// <param name="type">Target type. Must not be null.</param>
        /// <param name="options">Options. Must not be null.</param>
        /// <returns>The value; may be null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when type or options is null.</exception>
        /// <exception cref="JsonException">Thrown when the JSON is invalid for the type.</exception>
        /// <exception cref="NotSupportedException">Thrown when no JSON metadata is available for the type.</exception>
        public static object? Deserialize(ReadOnlySpan<byte> utf8Json, Type type, JsonSerializerOptions options)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(options);
            return JsonSerializer.Deserialize(utf8Json, GetTypeInfo(type, options));
        }

        private static JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            if (options.TypeInfoResolver == null && !options.IsReadOnly && JsonSerializer.IsReflectionEnabledByDefault)
                UseReflectionResolver(options);

            try
            {
                return options.GetTypeInfo(type);
            }
            catch (InvalidOperationException ex) when (options.TypeInfoResolver == null)
            {
                throw new NotSupportedException(
                    "JSON column type " + type.FullName + " cannot be serialized: reflection-based JSON serialization is disabled " +
                    "(trimmed or Native AOT application) and the JsonSerializerOptions have no TypeInfoResolver. Pass options created with " +
                    "DurableJson.CreateOptions(MyJsonContext.Default), where MyJsonContext is a JsonSerializerContext declaring " +
                    "[JsonSerializable(typeof(" + type.Name + "))].", ex);
            }
            catch (NotSupportedException ex)
            {
                throw new NotSupportedException(
                    "JSON column type " + type.FullName + " has no JSON metadata in the configured TypeInfoResolver. Add " +
                    "[JsonSerializable(typeof(" + type.Name + "))] to your JsonSerializerContext.", ex);
            }
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Only called when JsonSerializer.IsReflectionEnabledByDefault is true. That feature switch is false in trimmed and Native AOT applications (and lets the trimmer remove this call), where a source-generated TypeInfoResolver is required instead.")]
        [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Only called when JsonSerializer.IsReflectionEnabledByDefault is true, which is never the case under Native AOT.")]
        private static void UseReflectionResolver(JsonSerializerOptions options)
        {
            // Matches what the Type-based JsonSerializer overloads do on first use: freeze the options with the
            // reflection-based resolver.
            options.MakeReadOnly(populateMissingResolver: true);
        }
    }
}
