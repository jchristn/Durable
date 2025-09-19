namespace Test.Sqlite
{
    using System;
    using System.Reflection;
    using Durable;
    using Xunit;

    public class VersionColumnInfoTests
    {
        [Fact]
        public void IncrementVersion_IntegerType_IncrementsCorrectly()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.Integer,
                PropertyType = typeof(int)
            };
            
            object result = versionInfo.IncrementVersion(5);
            Assert.Equal(6, result);
        }
        
        [Fact]
        public void IncrementVersion_LongType_IncrementsCorrectly()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.Integer,
                PropertyType = typeof(long)
            };
            
            object result = versionInfo.IncrementVersion(99L);
            Assert.Equal(100L, result);
        }
        
        [Fact]
        public void IncrementVersion_ShortType_IncrementsCorrectly()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version", 
                Type = VersionColumnType.Integer,
                PropertyType = typeof(short)
            };
            
            object result = versionInfo.IncrementVersion((short)10);
            Assert.Equal((short)11, result);
        }
        
        [Fact]
        public void IncrementVersion_ByteType_IncrementsCorrectly()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.Integer,
                PropertyType = typeof(byte)
            };
            
            object result = versionInfo.IncrementVersion((byte)254);
            Assert.Equal((byte)255, result);
        }
        
        [Fact]
        public void IncrementVersion_TimestampType_ReturnsCurrentUtcTime()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.Timestamp,
                PropertyType = typeof(DateTime)
            };
            
            DateTime before = DateTime.UtcNow;
            object result = versionInfo.IncrementVersion(DateTime.UtcNow.AddDays(-1));
            DateTime after = DateTime.UtcNow;
            
            Assert.IsType<DateTime>(result);
            DateTime resultDateTime = (DateTime)result;
            Assert.True(resultDateTime >= before && resultDateTime <= after);
        }
        
        [Fact]
        public void IncrementVersion_GuidType_GeneratesNewGuid()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.Guid,
                PropertyType = typeof(Guid)
            };
            
            Guid originalGuid = Guid.NewGuid();
            object result = versionInfo.IncrementVersion(originalGuid);
            
            Assert.IsType<Guid>(result);
            Assert.NotEqual(originalGuid, (Guid)result);
        }
        
        [Fact]
        public void IncrementVersion_RowVersionType_IncrementsBytes()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.RowVersion,
                PropertyType = typeof(byte[])
            };
            
            byte[] original = new byte[] { 0, 0, 0, 0, 0, 0, 0, 5 };
            object result = versionInfo.IncrementVersion(original);
            
            Assert.IsType<byte[]>(result);
            byte[] resultBytes = (byte[])result;
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 6 }, resultBytes);
        }
        
        [Fact]
        public void IncrementVersion_RowVersionType_HandlesOverflow()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.RowVersion,
                PropertyType = typeof(byte[])
            };
            
            byte[] original = new byte[] { 0, 0, 0, 0, 0, 0, 0, 255 };
            object result = versionInfo.IncrementVersion(original);
            
            Assert.IsType<byte[]>(result);
            byte[] resultBytes = (byte[])result;
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 1, 0 }, resultBytes);
        }
        
        [Fact]
        public void IncrementVersion_RowVersionType_HandlesMaxValue()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.RowVersion,
                PropertyType = typeof(byte[])
            };
            
            byte[] original = new byte[] { 255, 255, 255, 255, 255, 255, 255, 255 };
            object result = versionInfo.IncrementVersion(original);
            
            Assert.IsType<byte[]>(result);
            byte[] resultBytes = (byte[])result;
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, resultBytes);
        }
        
        [Fact]
        public void IncrementVersion_NullValue_ReturnsDefaultVersion()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                ColumnName = "version",
                Type = VersionColumnType.Integer,
                PropertyType = typeof(int)
            };
            
            object result = versionInfo.IncrementVersion(null);
            Assert.Equal(1, result);
        }
        
        [Fact]
        public void GetDefaultVersion_IntegerType_ReturnsOne()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Integer,
                PropertyType = typeof(int)
            };
            
            object result = versionInfo.GetDefaultVersion();
            Assert.Equal(1, result);
        }
        
        [Fact]
        public void GetDefaultVersion_LongType_ReturnsOne()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Integer,
                PropertyType = typeof(long)
            };
            
            object result = versionInfo.GetDefaultVersion();
            Assert.Equal(1L, result);
        }
        
        [Fact]
        public void GetDefaultVersion_ShortType_ReturnsOne()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Integer,
                PropertyType = typeof(short)
            };
            
            object result = versionInfo.GetDefaultVersion();
            Assert.Equal((short)1, result);
        }
        
        [Fact]
        public void GetDefaultVersion_ByteType_ReturnsOne()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Integer,
                PropertyType = typeof(byte)
            };
            
            object result = versionInfo.GetDefaultVersion();
            Assert.Equal((byte)1, result);
        }
        
        [Fact]
        public void GetDefaultVersion_TimestampType_ReturnsCurrentUtcTime()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Timestamp,
                PropertyType = typeof(DateTime)
            };
            
            DateTime before = DateTime.UtcNow;
            object result = versionInfo.GetDefaultVersion();
            DateTime after = DateTime.UtcNow;
            
            Assert.IsType<DateTime>(result);
            DateTime resultDateTime = (DateTime)result;
            Assert.True(resultDateTime >= before && resultDateTime <= after);
        }
        
        [Fact]
        public void GetDefaultVersion_GuidType_ReturnsNewGuid()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Guid,
                PropertyType = typeof(Guid)
            };
            
            object result = versionInfo.GetDefaultVersion();
            Assert.IsType<Guid>(result);
            Assert.NotEqual(Guid.Empty, (Guid)result);
        }
        
        [Fact]
        public void GetDefaultVersion_RowVersionType_ReturnsInitialBytes()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.RowVersion,
                PropertyType = typeof(byte[])
            };
            
            object result = versionInfo.GetDefaultVersion();
            Assert.IsType<byte[]>(result);
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }, (byte[])result);
        }
        
        [Fact]
        public void FormatVersionForSql_IntegerValue_ReturnsStringValue()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Integer
            };
            
            string result = versionInfo.FormatVersionForSql(42);
            Assert.Equal("42", result);
        }
        
        [Fact]
        public void FormatVersionForSql_TimestampValue_ReturnsFormattedString()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Timestamp
            };
            
            DateTime testTime = new DateTime(2024, 1, 15, 10, 30, 45, 123, DateTimeKind.Utc).AddTicks(4567);
            string result = versionInfo.FormatVersionForSql(testTime);
            Assert.Equal("'2024-01-15 10:30:45.1234567'", result);
        }
        
        [Fact]
        public void FormatVersionForSql_RowVersionValue_ReturnsHexString()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.RowVersion
            };
            
            byte[] testBytes = new byte[] { 0, 15, 255, 128 };
            string result = versionInfo.FormatVersionForSql(testBytes);
            Assert.Equal("X'000FFF80'", result);
        }
        
        [Fact]
        public void FormatVersionForSql_GuidValue_ReturnsQuotedString()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Guid
            };
            
            Guid testGuid = new Guid("12345678-1234-5678-9abc-123456789abc");
            string result = versionInfo.FormatVersionForSql(testGuid);
            Assert.Equal("'12345678-1234-5678-9abc-123456789abc'", result);
        }
        
        [Fact]
        public void FormatVersionForSql_NullValue_ReturnsNull()
        {
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Type = VersionColumnType.Integer
            };
            
            string result = versionInfo.FormatVersionForSql(null);
            Assert.Equal("NULL", result);
        }
        
        [Fact]
        public void GetSetValue_WorksCorrectly()
        {
            VersionTestEntity entity = new VersionTestEntity();
            PropertyInfo property = typeof(VersionTestEntity).GetProperty(nameof(VersionTestEntity.Version))!;
            
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Property = property
            };
            
            versionInfo.SetValue(entity, 42);
            object result = versionInfo.GetValue(entity);
            
            Assert.Equal(42, result);
            Assert.Equal(42, entity.Version);
        }
        
        [Fact]
        public void GetValue_NullEntity_ReturnsNull()
        {
            PropertyInfo property = typeof(VersionTestEntity).GetProperty(nameof(VersionTestEntity.Version))!;
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Property = property
            };
            
            object result = versionInfo.GetValue(null);
            Assert.Null(result);
        }
        
        [Fact]
        public void SetValue_NullEntity_DoesNotThrow()
        {
            PropertyInfo property = typeof(VersionTestEntity).GetProperty(nameof(VersionTestEntity.Version))!;
            VersionColumnInfo versionInfo = new VersionColumnInfo
            {
                Property = property
            };
            
            versionInfo.SetValue(null, 42); // Should not throw
        }
    }
}