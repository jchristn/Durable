namespace Test.Benchmark
{
    using System;
    using System.Data;
    using Dapper;

    /// <summary>
    /// Lets Dapper read GUIDs that SQLite stores as text (native GUIDs from other databases pass through).
    /// </summary>
    public sealed class DapperGuidHandler : SqlMapper.TypeHandler<Guid>
    {
        /// <inheritdoc />
        public override Guid Parse(object value)
        {
            if (value is Guid guid) return guid;
            return Guid.Parse((string)value);
        }

        /// <inheritdoc />
        public override void SetValue(IDbDataParameter parameter, Guid value)
        {
            parameter.Value = value.ToString("D");
        }
    }
}
