namespace Durable.DefaultValueProviders
{
    using System;
    using System.Reflection;
    using System.Security.Cryptography;

    /// <summary>
    /// Provides a sequential GUID optimized for database indexing as a default value.
    /// Sequential GUIDs reduce index fragmentation compared to random GUIDs.
    /// </summary>
    public class SequentialGuidProvider : IDefaultValueProvider
    {
        private static readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();

        /// <inheritdoc/>
        public object? GetDefaultValue(PropertyInfo property, object entity)
        {
            return GenerateSequentialGuid();
        }

        /// <inheritdoc/>
        public bool ShouldApply(object? currentValue, Type propertyType)
        {
            if (currentValue == null) return true;

            if (propertyType == typeof(Guid) || propertyType == typeof(Guid?))
            {
                Guid guid = (currentValue is Guid g) ? g : default;
                return guid == default;
            }

            return false;
        }

        /// <summary>
        /// Generates a sequential GUID by combining timestamp with random bytes
        /// </summary>
        private static Guid GenerateSequentialGuid()
        {
            byte[] guidBytes = new byte[16];
            _rng.GetBytes(guidBytes);

            // Get timestamp as ticks (8 bytes)
            long timestamp = DateTime.UtcNow.Ticks;
            byte[] timestampBytes = BitConverter.GetBytes(timestamp);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(timestampBytes);
            }

            // Replace the last 6 bytes with timestamp bytes (most significant)
            // This ensures sequential ordering while maintaining uniqueness
            Buffer.BlockCopy(timestampBytes, 2, guidBytes, 10, 6);

            return new Guid(guidBytes);
        }
    }
}
