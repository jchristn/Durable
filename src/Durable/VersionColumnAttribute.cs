using System;

namespace Durable
{
    [AttributeUsage(AttributeTargets.Property)]
    public class VersionColumnAttribute : Attribute
    {
        public VersionColumnType Type { get; }
        
        public VersionColumnAttribute(VersionColumnType type = VersionColumnType.RowVersion)
        {
            Type = type;
        }
    }
}