namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    internal class IncludeInfo
    {

        public string PropertyPath { get; set; }
        public PropertyInfo NavigationProperty { get; set; }
        public PropertyInfo ForeignKeyProperty { get; set; }
        public Type RelatedEntityType { get; set; }
        public string RelatedTableName { get; set; }
        public string JoinAlias { get; set; }
        public IncludeInfo Parent { get; set; }
        public List<IncludeInfo> Children { get; set; } = new List<IncludeInfo>();
        public bool IsCollection { get; set; }
        public bool IsManyToMany { get; set; }
        public Type JunctionEntityType { get; set; }
        public string JunctionTableName { get; set; }
        public string JunctionAlias { get; set; }
        
    }
}