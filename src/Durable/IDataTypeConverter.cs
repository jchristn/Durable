namespace Durable
{
    using System;
    using System.Reflection;

    public interface IDataTypeConverter
    {
        object ConvertToDatabase(object value, Type targetType, PropertyInfo propertyInfo = null);
        
        object ConvertFromDatabase(object value, Type targetType, PropertyInfo propertyInfo = null);
        
        bool CanConvert(Type type);
        
        string GetDatabaseTypeString(Type type, PropertyInfo propertyInfo = null);
    }
}