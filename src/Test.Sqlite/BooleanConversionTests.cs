namespace Test.Sqlite
{
    using System;
    using Durable;
    using Xunit;

    /// <summary>
    /// Tests for boolean value conversion in the base DataTypeConverter.
    /// Verifies that various input formats (numeric, string) are correctly converted to boolean values.
    /// </summary>
    public class BooleanConversionTests
    {
        private readonly DataTypeConverter _Converter;

        /// <summary>
        /// Initializes a new instance of the <see cref="BooleanConversionTests"/> class.
        /// </summary>
        public BooleanConversionTests()
        {
            _Converter = new DataTypeConverter();
        }

        // ==================== NUMERIC TO BOOLEAN TESTS ====================

        /// <summary>
        /// Tests that integer 1 converts to true.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_IntegerOne_ReturnsTrue()
        {
            object result = _Converter.ConvertFromDatabase(1, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that integer 0 converts to false.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_IntegerZero_ReturnsFalse()
        {
            object result = _Converter.ConvertFromDatabase(0, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        /// <summary>
        /// Tests that long 1 converts to true.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_LongOne_ReturnsTrue()
        {
            object result = _Converter.ConvertFromDatabase(1L, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that long 0 converts to false.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_LongZero_ReturnsFalse()
        {
            object result = _Converter.ConvertFromDatabase(0L, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        /// <summary>
        /// Tests that non-zero integers convert to true.
        /// </summary>
        [Theory]
        [InlineData(2)]
        [InlineData(-1)]
        [InlineData(100)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        public void ConvertFromDatabase_NonZeroInteger_ReturnsTrue(int value)
        {
            object result = _Converter.ConvertFromDatabase(value, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that byte 1 converts to true.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_ByteOne_ReturnsTrue()
        {
            object result = _Converter.ConvertFromDatabase((byte)1, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that byte 0 converts to false.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_ByteZero_ReturnsFalse()
        {
            object result = _Converter.ConvertFromDatabase((byte)0, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        /// <summary>
        /// Tests that sbyte 1 converts to true.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_SbyteOne_ReturnsTrue()
        {
            object result = _Converter.ConvertFromDatabase((sbyte)1, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that sbyte 0 converts to false.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_SbyteZero_ReturnsFalse()
        {
            object result = _Converter.ConvertFromDatabase((sbyte)0, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        /// <summary>
        /// Tests that short 1 converts to true.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_ShortOne_ReturnsTrue()
        {
            object result = _Converter.ConvertFromDatabase((short)1, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that short 0 converts to false.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_ShortZero_ReturnsFalse()
        {
            object result = _Converter.ConvertFromDatabase((short)0, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        // ==================== STRING TO BOOLEAN TESTS ====================

        /// <summary>
        /// Tests that string "true" converts to true (case-insensitive).
        /// </summary>
        [Theory]
        [InlineData("true")]
        [InlineData("True")]
        [InlineData("TRUE")]
        [InlineData("tRuE")]
        public void ConvertFromDatabase_StringTrue_ReturnsTrue(string value)
        {
            object result = _Converter.ConvertFromDatabase(value, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that string "false" converts to false (case-insensitive).
        /// </summary>
        [Theory]
        [InlineData("false")]
        [InlineData("False")]
        [InlineData("FALSE")]
        [InlineData("fAlSe")]
        public void ConvertFromDatabase_StringFalse_ReturnsFalse(string value)
        {
            object result = _Converter.ConvertFromDatabase(value, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        /// <summary>
        /// Tests that string "1" converts to true.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_StringOne_ReturnsTrue()
        {
            object result = _Converter.ConvertFromDatabase("1", typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that string "0" converts to false.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_StringZero_ReturnsFalse()
        {
            object result = _Converter.ConvertFromDatabase("0", typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        /// <summary>
        /// Tests that other non-false strings convert to true.
        /// </summary>
        [Theory]
        [InlineData("yes")]
        [InlineData("Y")]
        [InlineData("on")]
        [InlineData("enabled")]
        public void ConvertFromDatabase_OtherStrings_ReturnsTrue(string value)
        {
            object result = _Converter.ConvertFromDatabase(value, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        // ==================== NATIVE BOOLEAN TESTS ====================

        /// <summary>
        /// Tests that native bool true passes through correctly.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_NativeBoolTrue_ReturnsTrue()
        {
            object result = _Converter.ConvertFromDatabase(true, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that native bool false passes through correctly.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_NativeBoolFalse_ReturnsFalse()
        {
            object result = _Converter.ConvertFromDatabase(false, typeof(bool));
            Assert.IsType<bool>(result);
            Assert.False((bool)result!);
        }

        // ==================== NULLABLE BOOLEAN TESTS ====================

        /// <summary>
        /// Tests that null value converts to null for nullable bool.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_NullValue_ReturnsNullForNullableBool()
        {
            object? result = _Converter.ConvertFromDatabase(null, typeof(bool?));
            Assert.Null(result);
        }

        /// <summary>
        /// Tests that DBNull value converts to null for nullable bool.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_DbNull_ReturnsNullForNullableBool()
        {
            object? result = _Converter.ConvertFromDatabase(DBNull.Value, typeof(bool?));
            Assert.Null(result);
        }

        /// <summary>
        /// Tests that integer 1 converts to true for nullable bool.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_IntegerOne_ReturnsTrueForNullableBool()
        {
            object? result = _Converter.ConvertFromDatabase(1, typeof(bool?));
            Assert.NotNull(result);
            Assert.True((bool)result!);
        }

        /// <summary>
        /// Tests that string "false" converts to false for nullable bool.
        /// </summary>
        [Fact]
        public void ConvertFromDatabase_StringFalse_ReturnsFalseForNullableBool()
        {
            object? result = _Converter.ConvertFromDatabase("false", typeof(bool?));
            Assert.NotNull(result);
            Assert.False((bool)result!);
        }
    }
}
