namespace Durable.Tool
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;

    /// <summary>
    /// C# naming and literal helpers for generated code.
    /// </summary>
    internal static class CSharpNames
    {
        private static readonly HashSet<string> _Keywords = new HashSet<string>(StringComparer.Ordinal)
        {
            "abstract", "as", "base", "bool", "break", "byte", "case", "catch", "char", "checked", "class", "const", "continue",
            "decimal", "default", "delegate", "do", "double", "else", "enum", "event", "explicit", "extern", "false", "finally",
            "fixed", "float", "for", "foreach", "goto", "if", "implicit", "in", "int", "interface", "internal", "is", "lock",
            "long", "namespace", "new", "null", "object", "operator", "out", "override", "params", "private", "protected",
            "public", "readonly", "ref", "return", "sbyte", "sealed", "short", "sizeof", "stackalloc", "static", "string",
            "struct", "switch", "this", "throw", "true", "try", "typeof", "uint", "ulong", "unchecked", "unsafe", "ushort",
            "using", "virtual", "void", "volatile", "while"
        };

        /// <summary>
        /// Returns whether a name is a valid, non-keyword C# identifier (ASCII letters, digits and underscores).
        /// </summary>
        /// <param name="name">Name; null is invalid.</param>
        /// <returns>True when valid.</returns>
        public static bool IsValidIdentifier(string? name)
        {
            if (string.IsNullOrEmpty(name) || _Keywords.Contains(name)) return false;
            if (!(char.IsLetter(name[0]) || name[0] == '_')) return false;
            return name.All(c => char.IsLetterOrDigit(c) || c == '_');
        }

        /// <summary>
        /// Returns whether a dotted namespace is valid.
        /// </summary>
        /// <param name="value">Namespace; null is invalid.</param>
        /// <returns>True when every segment is a valid identifier.</returns>
        public static bool IsValidNamespace(string? value)
        {
            return !string.IsNullOrEmpty(value) && value.Split('.').All(IsValidIdentifier);
        }

        /// <summary>
        /// Converts a database name such as <c>order_items</c> or <c>CustomerID</c> to a PascalCase identifier
        /// (<c>OrderItems</c>, <c>CustomerID</c>). Names starting with a digit are prefixed.
        /// </summary>
        /// <param name="name">Name. Must not be null.</param>
        /// <param name="prefix">Prefix used when the result would start with a digit or be empty. Must not be null.</param>
        /// <returns>The identifier.</returns>
        public static string ToPascalCase(string name, string prefix)
        {
            ArgumentNullException.ThrowIfNull(name);
            StringBuilder result = new StringBuilder();
            foreach (string part in Regex.Split(name, "[^\\p{L}\\p{Nd}]+"))
            {
                if (part.Length == 0) continue;
                bool allUpper = part.All(c => !char.IsLetter(c) || char.IsUpper(c));
                string rest = allUpper && part.Length > 1 ? part.Substring(1).ToLowerInvariant() : part.Substring(1);
                result.Append(char.ToUpperInvariant(part[0])).Append(rest);
            }

            string text = result.ToString();
            if (text.Length == 0 || char.IsDigit(text[0])) text = prefix + text;
            return text;
        }

        /// <summary>
        /// Returns a simple English singular of a plural table name (customers -> customer, categories -> category,
        /// addresses -> address). Names that do not look plural are returned unchanged.
        /// </summary>
        /// <param name="name">Name. Must not be null.</param>
        /// <returns>The singular name.</returns>
        public static string Singularize(string name)
        {
            ArgumentNullException.ThrowIfNull(name);
            string lower = name.ToLowerInvariant();
            if (lower.EndsWith("ies", StringComparison.Ordinal) && name.Length > 4) return name.Substring(0, name.Length - 3) + (char.IsUpper(name[^1]) ? "Y" : "y");
            if (lower.EndsWith("sses", StringComparison.Ordinal) || lower.EndsWith("xes", StringComparison.Ordinal) || lower.EndsWith("ches", StringComparison.Ordinal) ||
                lower.EndsWith("shes", StringComparison.Ordinal) || lower.EndsWith("zzes", StringComparison.Ordinal))
                return name.Substring(0, name.Length - 2);
            if (lower.EndsWith("s", StringComparison.Ordinal) && !lower.EndsWith("ss", StringComparison.Ordinal) && !lower.EndsWith("us", StringComparison.Ordinal) &&
                !lower.EndsWith("is", StringComparison.Ordinal) && name.Length > 3)
                return name.Substring(0, name.Length - 1);
            return name;
        }

        /// <summary>
        /// Returns a C# string literal for a value, escaping quotes, backslashes and control characters.
        /// </summary>
        /// <param name="value">Value. Must not be null.</param>
        /// <returns>The literal including quotes.</returns>
        public static string StringLiteral(string value)
        {
            ArgumentNullException.ThrowIfNull(value);
            StringBuilder sb = new StringBuilder("\"");
            foreach (char c in value)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\t': sb.Append("\\t"); break;
                    case '\0': sb.Append("\\0"); break;
                    default:
                        if (char.IsControl(c)) sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        else sb.Append(c);
                        break;
                }
            }

            return sb.Append('"').ToString();
        }

        /// <summary>
        /// Escapes text for use inside an XML documentation comment.
        /// </summary>
        /// <param name="value">Text. Must not be null.</param>
        /// <returns>The escaped text.</returns>
        public static string XmlEscape(string value)
        {
            ArgumentNullException.ThrowIfNull(value);
            return value.Replace("&", "&amp;", StringComparison.Ordinal).Replace("<", "&lt;", StringComparison.Ordinal).Replace(">", "&gt;", StringComparison.Ordinal);
        }

        /// <summary>
        /// Returns a name made unique against a set of used names by appending a number; the result is added to the set.
        /// </summary>
        /// <param name="name">Preferred name. Must not be null.</param>
        /// <param name="used">Names already used. Must not be null.</param>
        /// <returns>The unique name.</returns>
        public static string MakeUnique(string name, HashSet<string> used)
        {
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(used);
            string candidate = name;
            int suffix = 2;
            while (!used.Add(candidate)) candidate = name + suffix++.ToString(CultureInfo.InvariantCulture);
            return candidate;
        }
    }
}
