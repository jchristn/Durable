namespace Durable.Tool
{
    using System;
    using System.Text;

    /// <summary>
    /// Builds indented C# source text (four spaces per level).
    /// </summary>
    internal sealed class CodeWriter
    {
        private readonly StringBuilder _Text = new StringBuilder();
        private int _Indent = 0;

        /// <summary>
        /// Writes a line at the current indentation; an empty string writes a blank line.
        /// </summary>
        /// <param name="line">Line text. Must not be null.</param>
        /// <returns>This writer.</returns>
        public CodeWriter Line(string line)
        {
            ArgumentNullException.ThrowIfNull(line);
            if (line.Length > 0) _Text.Append(' ', _Indent * 4).Append(line);
            _Text.Append('\n');
            return this;
        }

        /// <summary>
        /// Writes a blank line.
        /// </summary>
        /// <returns>This writer.</returns>
        public CodeWriter Blank()
        {
            _Text.Append('\n');
            return this;
        }

        /// <summary>
        /// Writes an opening brace and indents.
        /// </summary>
        /// <returns>This writer.</returns>
        public CodeWriter Open()
        {
            Line("{");
            _Indent++;
            return this;
        }

        /// <summary>
        /// Outdents and writes a closing brace.
        /// </summary>
        /// <returns>This writer.</returns>
        /// <exception cref="InvalidOperationException">Thrown when there is no open block.</exception>
        public CodeWriter Close()
        {
            if (_Indent == 0) throw new InvalidOperationException("No open block to close.");
            _Indent--;
            return Line("}");
        }

        /// <summary>
        /// Writes an XML documentation summary.
        /// </summary>
        /// <param name="lines">Summary lines (already XML-escaped). Must not be null.</param>
        /// <returns>This writer.</returns>
        public CodeWriter Summary(params string[] lines)
        {
            ArgumentNullException.ThrowIfNull(lines);
            Line("/// <summary>");
            foreach (string line in lines) Line("/// " + line);
            return Line("/// </summary>");
        }

        /// <summary>
        /// Returns the source text.
        /// </summary>
        /// <returns>The text.</returns>
        public override string ToString()
        {
            return _Text.ToString();
        }
    }
}
