namespace Durable
{
    using System;
    using System.Reflection;
    
    public class VersionColumnInfo
    {
        #region Public-Members

        public string ColumnName { get; set; } = null!;
        public PropertyInfo Property { get; set; } = null!;
        public VersionColumnType Type { get; set; }
        public Type PropertyType { get; set; } = null!;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        public object? GetValue(object entity)
        {
            if (entity == null)
                return null;
                
            return Property.GetValue(entity);
        }
        
        public void SetValue(object entity, object value)
        {
            if (entity == null)
                return;
                
            Property.SetValue(entity, value);
        }
        
        public object IncrementVersion(object currentVersion)
        {
            if (currentVersion == null)
            {
                return GetDefaultVersion();
            }
            
            switch (Type)
            {
                case VersionColumnType.Integer:
                    if (PropertyType == typeof(int))
                        return (int)currentVersion + 1;
                    else if (PropertyType == typeof(long))
                        return (long)currentVersion + 1;
                    else if (PropertyType == typeof(short))
                        return (short)((short)currentVersion + 1);
                    else if (PropertyType == typeof(byte))
                        return (byte)((byte)currentVersion + 1);
                    break;
                    
                case VersionColumnType.Timestamp:
                    return DateTime.UtcNow;
                    
                case VersionColumnType.RowVersion:
                    byte[] currentBytes = (byte[])currentVersion;
                    byte[] newBytes = new byte[currentBytes.Length];
                    Array.Copy(currentBytes, newBytes, currentBytes.Length);
                    for (int i = newBytes.Length - 1; i >= 0; i--)
                    {
                        if (newBytes[i] < 255)
                        {
                            newBytes[i]++;
                            break;
                        }
                        newBytes[i] = 0;
                    }
                    return newBytes;
                    
                case VersionColumnType.Guid:
                    return Guid.NewGuid();
            }
            
            return currentVersion;
        }
        
        public object GetDefaultVersion()
        {
            switch (Type)
            {
                case VersionColumnType.Integer:
                    if (PropertyType == typeof(int))
                        return 1;
                    else if (PropertyType == typeof(long))
                        return 1L;
                    else if (PropertyType == typeof(short))
                        return (short)1;
                    else if (PropertyType == typeof(byte))
                        return (byte)1;
                    break;
                    
                case VersionColumnType.Timestamp:
                    return DateTime.UtcNow;
                    
                case VersionColumnType.RowVersion:
                    return new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
                    
                case VersionColumnType.Guid:
                    return Guid.NewGuid();
            }
            
            return null!;
        }
        
        public string FormatVersionForSql(object version)
        {
            if (version == null)
                return "NULL";
                
            switch (Type)
            {
                case VersionColumnType.Integer:
                    return version.ToString() ?? "NULL";
                    
                case VersionColumnType.Timestamp:
                    DateTime dt = (DateTime)version;
                    return $"'{dt:yyyy-MM-dd HH:mm:ss.fffffff}'";
                    
                case VersionColumnType.RowVersion:
                    byte[] bytes = (byte[])version;
                    return $"X'{BitConverter.ToString(bytes).Replace("-", "")}'";
                    
                case VersionColumnType.Guid:
                    return $"'{version}'";
            }
            
            return "NULL";
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}