namespace Test.Aot
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Durable;

    /// <summary>
    /// Translates <see cref="ExternalTableAttribute"/> and <see cref="ExternalColumnAttribute"/> into Durable's attributes.
    /// </summary>
    public sealed class ExternalMappingSource : IEntityMappingSource
    {
        public bool Describes(Type entityType)
        {
            return entityType.GetCustomAttribute<ExternalTableAttribute>() != null;
        }

        public EntityAttribute? GetEntityAttribute(Type entityType)
        {
            ExternalTableAttribute? table = entityType.GetCustomAttribute<ExternalTableAttribute>();
            return table != null ? new EntityAttribute(table.Name) : null;
        }

        public IEnumerable<Attribute>? GetPropertyAttributes(Type entityType, PropertyInfo property)
        {
            ExternalColumnAttribute? column = property.GetCustomAttribute<ExternalColumnAttribute>();
            if (column == null) return null;
            List<Attribute> result = new List<Attribute>
            {
                new PropertyAttribute(column.Name, column.Key ? Flags.PrimaryKey | Flags.AutoIncrement : Flags.None)
            };
            if (column.Isbn) result.Add(new ValueConverterAttribute(typeof(IsbnConverter)));
            return result;
        }

        public IEnumerable<CompositeIndexAttribute>? GetCompositeIndexes(Type entityType)
        {
            return null;
        }
    }
}
