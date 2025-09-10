namespace Durable
{
    using System;
    using System.Reflection;
    
    /// <summary>
    /// Contains information about a version column property
    /// </summary>
    public class VersionColumnInfo
    {
        #region Public-Members

        /// <summary>
        /// Gets or sets the database column name
        /// </summary>
        public string ColumnName { get; set; } = null!;
        /// <summary>
        /// Gets or sets the property information
        /// </summary>
        public PropertyInfo Property { get; set; } = null!;
        /// <summary>
        /// Gets or sets the version column type
        /// </summary>
        public VersionColumnType Type { get; set; }
        /// <summary>
        /// Gets or sets the property type
        /// </summary>
        public Type PropertyType { get; set; } = null!;

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Gets the version value from the specified entity
        /// </summary>
        /// <param name="entity">The entity to get the version from</param>
        /// <returns>The version value</returns>
        public object? GetValue(object entity)
        {
            if (entity == null)
                return null;
                
            return Property.GetValue(entity);
        }
        
        /// <summary>
        /// Sets the version value on the specified entity
        /// </summary>
        /// <param name="entity">The entity to set the version on</param>
        /// <param name="value">The version value to set</param>
        public void SetValue(object entity, object value)
        {
            if (entity == null)
                return;
                
            Property.SetValue(entity, value);
        }
        
        /// <summary>
        /// Increments the version value based on the version column type
        /// </summary>
        /// <param name="currentVersion">The current version value</param>
        /// <returns>The incremented version value</returns>
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
        
        /// <summary>
        /// Gets the default version value based on the version column type
        /// </summary>
        /// <returns>The default version value</returns>
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
        
        /// <summary>
        /// Formats the version value for SQL queries
        /// </summary>
        /// <param name="version">The version value to format</param>
        /// <returns>The SQL-formatted version string</returns>
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