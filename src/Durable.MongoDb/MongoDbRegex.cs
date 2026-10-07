namespace Durable.MongoDb
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text;
    using Durable;
    using MongoDB.Bson;

    /// <summary>
    /// Builds MongoDB regular expressions that match literal text exactly as C# does, for string matches
    /// (<c>Contains</c>, <c>StartsWith</c>, <c>EndsWith</c>) and case-insensitive equality pushed down to the server.
    /// <para>
    /// Every character of the literal is escaped (letters and digits of ASCII stay readable; everything else is written as
    /// <c>\x{hhhh}</c>), so regex metacharacters never apply. MongoDB matches code points of UTF-8 strings, which equals C#
    /// ordinal matching of UTF-16 strings for well-formed text; a literal containing a lone surrogate is not translated.
    /// </para>
    /// <para>
    /// Case-insensitive patterns do not use the regex <c>i</c> option, whose Unicode case folding differs from
    /// <see cref="StringComparison.OrdinalIgnoreCase"/>. Instead each character becomes a class of exactly the characters that
    /// <see cref="StringComparison.OrdinalIgnoreCase"/> considers equal to it (for example <c>[Kk]</c>, or
    /// <c>[Iiı]</c>-style classes where the runtime's casing tables say so), computed once from the running .NET runtime.
    /// That is exact for literals of the Basic Multilingual Plane; a case-insensitive literal containing a supplementary
    /// character is not translated.
    /// </para>
    /// Thread safety: stateless apart from thread-safe caches; safe for concurrent use.
    /// </summary>
    internal static class MongoDbRegex
    {
        #region Private-Members

        private static readonly Lazy<Dictionary<char, List<char>>> _UpperGroups = new Lazy<Dictionary<char, List<char>>>(BuildUpperGroups);
        private static readonly ConcurrentDictionary<char, string> _Classes = new ConcurrentDictionary<char, string>();

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds a regular expression for a string match.
        /// </summary>
        /// <param name="literal">Literal text. Must not be null.</param>
        /// <param name="kind">Contains, StartsWith or EndsWith; null for whole-string equality.</param>
        /// <param name="ignoreCase">Whether to match with <see cref="StringComparison.OrdinalIgnoreCase"/>.</param>
        /// <returns>The expression, or null when the literal cannot be matched exactly (see the class remarks).</returns>
        public static BsonRegularExpression? Build(string literal, Durable.Query.StringMatchKind? kind, bool ignoreCase)
        {
            ArgumentNullException.ThrowIfNull(literal);
            StringBuilder pattern = new StringBuilder(literal.Length * 2 + 4);
            if (kind == null || kind == Durable.Query.StringMatchKind.StartsWith) pattern.Append('^');
            for (int i = 0; i < literal.Length; i++)
            {
                char c = literal[i];
                if (char.IsSurrogate(c))
                {
                    if (ignoreCase || !char.IsHighSurrogate(c) || i + 1 >= literal.Length || !char.IsLowSurrogate(literal[i + 1])) return null;
                    AppendCodePoint(pattern, char.ConvertToUtf32(c, literal[i + 1]));
                    i++;
                    continue;
                }

                if (ignoreCase) pattern.Append(ClassFor(c));
                else AppendCodePoint(pattern, c);
            }

            if (kind == null || kind == Durable.Query.StringMatchKind.EndsWith) pattern.Append("\\z");
            return new BsonRegularExpression(pattern.ToString());
        }

        /// <summary>
        /// Returns whether ordinal range comparisons (&lt;, &lt;=, &gt;, &gt;=) against a literal give the same result in MongoDB
        /// (UTF-8 byte order) as in C# (UTF-16 code unit order): true when the literal has no character at or above U+D800,
        /// because the two orders only disagree between supplementary characters and U+E000 to U+FFFF.
        /// </summary>
        /// <param name="literal">Literal. Must not be null.</param>
        /// <returns>True when range comparisons are exact.</returns>
        public static bool IsRangeSafe(string literal)
        {
            foreach (char c in literal)
            {
                if (c >= '\uD800') return false;
            }

            return true;
        }

        #endregion

        #region Private-Methods

        private static string ClassFor(char c)
        {
            return _Classes.GetOrAdd(c, key =>
            {
                List<char> variants = new List<char> { key };
                if (_UpperGroups.Value.TryGetValue(char.ToUpperInvariant(key), out List<char>? group))
                {
                    foreach (char candidate in group)
                    {
                        if (candidate != key && string.Equals(key.ToString(), candidate.ToString(), StringComparison.OrdinalIgnoreCase)) variants.Add(candidate);
                    }
                }

                StringBuilder text = new StringBuilder();
                if (variants.Count > 1) text.Append('[');
                foreach (char variant in variants) AppendCodePoint(text, variant);
                if (variants.Count > 1) text.Append(']');
                return text.ToString();
            });
        }

        private static Dictionary<char, List<char>> BuildUpperGroups()
        {
            Dictionary<char, List<char>> groups = new Dictionary<char, List<char>>();
            for (int i = 0; i <= char.MaxValue; i++)
            {
                char c = (char)i;
                if (char.IsSurrogate(c)) continue;
                char upper = char.ToUpperInvariant(c);
                if (!groups.TryGetValue(upper, out List<char>? group))
                {
                    group = new List<char>(2);
                    groups[upper] = group;
                }

                group.Add(c);
            }

            return groups;
        }

        private static void AppendCodePoint(StringBuilder pattern, int codePoint)
        {
            if ((codePoint >= 'a' && codePoint <= 'z') || (codePoint >= 'A' && codePoint <= 'Z') || (codePoint >= '0' && codePoint <= '9'))
            {
                pattern.Append((char)codePoint);
                return;
            }

            pattern.Append("\\x{").Append(codePoint.ToString("X", CultureInfo.InvariantCulture)).Append('}');
        }

        #endregion
    }
}
