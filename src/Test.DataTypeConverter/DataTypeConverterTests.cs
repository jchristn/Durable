using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using Durable;
using FluentAssertions;
using Xunit;

namespace Test.DataTypeConverter
{
    public class DataTypeConverterTests
    {
        private readonly Durable.DataTypeConverter _converter = new();

        [Fact]
        public void ConvertToDatabase_DateTime_ReturnsFormattedString()
        {
            var dateTime = new DateTime(2024, 3, 15, 14, 30, 45, 123, DateTimeKind.Utc);
            var result = _converter.ConvertToDatabase(dateTime, typeof(DateTime));
            
            result.Should().Be("2024-03-15 14:30:45.1230000");
        }

        [Fact]
        public void ConvertFromDatabase_DateTimeString_ReturnsDateTime()
        {
            var dateString = "2024-03-15 14:30:45.1230000";
            var result = _converter.ConvertFromDatabase(dateString, typeof(DateTime));
            
            result.Should().BeOfType<DateTime>();
            var dateTime = (DateTime)result!;
            dateTime.Year.Should().Be(2024);
            dateTime.Month.Should().Be(3);
            dateTime.Day.Should().Be(15);
            dateTime.Hour.Should().Be(14);
            dateTime.Minute.Should().Be(30);
            dateTime.Second.Should().Be(45);
            dateTime.Millisecond.Should().Be(123);
        }

        [Fact]
        public void ConvertToDatabase_DateTimeOffset_ReturnsFormattedStringWithOffset()
        {
            var dateTimeOffset = new DateTimeOffset(2024, 3, 15, 14, 30, 45, 123, TimeSpan.FromHours(-5));
            var result = _converter.ConvertToDatabase(dateTimeOffset, typeof(DateTimeOffset));
            
            result.Should().Be("2024-03-15 14:30:45.1230000-05:00");
        }

        [Fact]
        public void ConvertFromDatabase_DateTimeOffsetString_ReturnsDateTimeOffset()
        {
            var dtoString = "2024-03-15 14:30:45.1230000-05:00";
            var result = _converter.ConvertFromDatabase(dtoString, typeof(DateTimeOffset));
            
            result.Should().BeOfType<DateTimeOffset>();
            var dto = (DateTimeOffset)result!;
            dto.Year.Should().Be(2024);
            dto.Offset.Should().Be(TimeSpan.FromHours(-5));
        }

        [Fact]
        public void ConvertToDatabase_TimeSpan_ReturnsFormattedString()
        {
            var timeSpan = new TimeSpan(1, 30, 45);
            var result = _converter.ConvertToDatabase(timeSpan, typeof(TimeSpan));
            
            result.Should().Be("01:30:45");
        }

        [Fact]
        public void ConvertFromDatabase_TimeSpanString_ReturnsTimeSpan()
        {
            var tsString = "01:30:45";
            var result = _converter.ConvertFromDatabase(tsString, typeof(TimeSpan));
            
            result.Should().BeOfType<TimeSpan>();
            var timeSpan = (TimeSpan)result!;
            timeSpan.Hours.Should().Be(1);
            timeSpan.Minutes.Should().Be(30);
            timeSpan.Seconds.Should().Be(45);
        }

        [Fact]
        public void ConvertToDatabase_Guid_ReturnsString()
        {
            var guid = Guid.NewGuid();
            var result = _converter.ConvertToDatabase(guid, typeof(Guid));
            
            result.Should().Be(guid.ToString());
        }

        [Fact]
        public void ConvertFromDatabase_GuidString_ReturnsGuid()
        {
            var guidString = "12345678-1234-1234-1234-123456789abc";
            var result = _converter.ConvertFromDatabase(guidString, typeof(Guid));
            
            result.Should().BeOfType<Guid>();
            ((Guid)result!).ToString().Should().Be(guidString);
        }

        [Fact]
        public void ConvertToDatabase_EnumAsString_ReturnsStringValue()
        {
            var status = TestEnum.Active;
            var result = _converter.ConvertToDatabase(status, typeof(TestEnum));
            
            result.Should().Be("Active");
        }

        [Fact]
        public void ConvertToDatabase_EnumAsInt_ReturnsIntValue()
        {
            var status = TestEnum.Pending;
            var prop = typeof(TestClass).GetProperty(nameof(TestClass.StatusAsInt));
            var result = _converter.ConvertToDatabase(status, typeof(TestEnum), prop);
            
            result.Should().Be(2);
        }

        [Fact]
        public void ConvertFromDatabase_StringToEnum_ReturnsEnumValue()
        {
            var result = _converter.ConvertFromDatabase("Active", typeof(TestEnum));
            
            result.Should().Be(TestEnum.Active);
        }

        [Fact]
        public void ConvertFromDatabase_IntToEnum_ReturnsEnumValue()
        {
            var result = _converter.ConvertFromDatabase(2, typeof(TestEnum));
            
            result.Should().Be(TestEnum.Pending);
        }

        [Fact]
        public void ConvertToDatabase_Array_ReturnsJsonString()
        {
            var array = new[] { "tag1", "tag2", "tag3" };
            var result = _converter.ConvertToDatabase(array, typeof(string[]));
            
            result.Should().Be("[\"tag1\",\"tag2\",\"tag3\"]");
        }

        [Fact]
        public void ConvertFromDatabase_JsonStringToArray_ReturnsArray()
        {
            var json = "[\"tag1\",\"tag2\",\"tag3\"]";
            var result = _converter.ConvertFromDatabase(json, typeof(string[]));
            
            result.Should().BeOfType<string[]>();
            var array = (string[])result!;
            array.Should().HaveCount(3);
            array[0].Should().Be("tag1");
            array[1].Should().Be("tag2");
            array[2].Should().Be("tag3");
        }

        [Fact]
        public void ConvertToDatabase_List_ReturnsJsonString()
        {
            var list = new List<int> { 1, 2, 3, 4 };
            var result = _converter.ConvertToDatabase(list, typeof(List<int>));
            
            result.Should().Be("[1,2,3,4]");
        }

        [Fact]
        public void ConvertFromDatabase_JsonStringToList_ReturnsList()
        {
            var json = "[1,2,3,4]";
            var result = _converter.ConvertFromDatabase(json, typeof(List<int>));
            
            result.Should().BeOfType<List<int>>();
            var list = (List<int>)result!;
            list.Should().HaveCount(4);
            list.Should().ContainInOrder(1, 2, 3, 4);
        }

        [Fact]
        public void ConvertToDatabase_ComplexObject_ReturnsJsonString()
        {
            var obj = new ComplexObject
            {
                Name = "Test",
                Value = 42,
                IsActive = true
            };
            var result = _converter.ConvertToDatabase(obj, typeof(ComplexObject));
            
            result.Should().BeOfType<string>();
            var jsonString = (string)result;
            jsonString.Should().Contain("\"name\":\"Test\"");
            jsonString.Should().Contain("\"value\":42");
            jsonString.Should().Contain("\"isActive\":true");
        }

        [Fact]
        public void ConvertFromDatabase_JsonStringToComplexObject_ReturnsObject()
        {
            var json = "{\"name\":\"Test\",\"value\":42,\"isActive\":true}";
            var result = _converter.ConvertFromDatabase(json, typeof(ComplexObject));
            
            result.Should().BeOfType<ComplexObject>();
            var obj = (ComplexObject)result!;
            obj.Name.Should().Be("Test");
            obj.Value.Should().Be(42);
            obj.IsActive.Should().BeTrue();
        }

        [Fact]
        public void ConvertToDatabase_NullValue_ReturnsDBNull()
        {
            var result = _converter.ConvertToDatabase(null!, typeof(string));
            
            result.Should().Be(DBNull.Value);
        }

        [Fact]
        public void ConvertFromDatabase_NullForNullableType_ReturnsNull()
        {
            var result = _converter.ConvertFromDatabase(null, typeof(int?));
            
            result.Should().BeNull();
        }

        [Fact]
        public void ConvertFromDatabase_NullForValueType_ReturnsDefault()
        {
            var result = _converter.ConvertFromDatabase(null, typeof(int));
            
            result.Should().Be(0);
        }

        [Fact]
        public void ConvertFromDatabase_DBNullForNullableType_ReturnsNull()
        {
            var result = _converter.ConvertFromDatabase(DBNull.Value, typeof(string));
            
            result.Should().BeNull();
        }

        [Fact]
        public void ConvertToDatabase_NullableIntWithValue_ReturnsValue()
        {
            int? value = 42;
            var result = _converter.ConvertToDatabase(value, typeof(int?));
            
            result.Should().Be(42);
        }

        [Fact]
        public void ConvertFromDatabase_ValueForNullableInt_ReturnsNullableInt()
        {
            var result = _converter.ConvertFromDatabase(42, typeof(int?));
            
            result.Should().Be(42);
        }

        [Fact]
        public void GetDatabaseTypeString_CommonTypes_ReturnsCorrectType()
        {
            _converter.GetDatabaseTypeString(typeof(int)).Should().Be("INTEGER");
            _converter.GetDatabaseTypeString(typeof(long)).Should().Be("INTEGER");
            _converter.GetDatabaseTypeString(typeof(bool)).Should().Be("INTEGER");
            _converter.GetDatabaseTypeString(typeof(double)).Should().Be("REAL");
            _converter.GetDatabaseTypeString(typeof(decimal)).Should().Be("REAL");
            _converter.GetDatabaseTypeString(typeof(string)).Should().Be("TEXT");
            _converter.GetDatabaseTypeString(typeof(DateTime)).Should().Be("TEXT");
            _converter.GetDatabaseTypeString(typeof(Guid)).Should().Be("TEXT");
            _converter.GetDatabaseTypeString(typeof(TimeSpan)).Should().Be("TEXT");
            _converter.GetDatabaseTypeString(typeof(string[])).Should().Be("TEXT");
        }

        [Fact]
        public void GetDatabaseTypeString_EnumWithoutAttribute_ReturnsText()
        {
            var result = _converter.GetDatabaseTypeString(typeof(TestEnum));
            
            result.Should().Be("TEXT");
        }

        [Fact]
        public void GetDatabaseTypeString_EnumWithIntegerAttribute_ReturnsInteger()
        {
            var prop = typeof(TestClass).GetProperty(nameof(TestClass.StatusAsInt));
            var result = _converter.GetDatabaseTypeString(typeof(TestEnum), prop);
            
            result.Should().Be("INTEGER");
        }

        [Fact]
        public void CanConvert_AlwaysReturnsTrue()
        {
            _converter.CanConvert(typeof(int)).Should().BeTrue();
            _converter.CanConvert(typeof(string)).Should().BeTrue();
            _converter.CanConvert(typeof(DateTime)).Should().BeTrue();
            _converter.CanConvert(typeof(ComplexObject)).Should().BeTrue();
        }

        [Fact]
        public void RoundTrip_DateTime_PreservesValue()
        {
            var original = DateTime.UtcNow;
            var dbValue = _converter.ConvertToDatabase(original, typeof(DateTime));
            var restored = _converter.ConvertFromDatabase(dbValue, typeof(DateTime));
            
            restored.Should().BeOfType<DateTime>();
            var restoredDate = (DateTime)restored!;
            
            (original - restoredDate).TotalMilliseconds.Should().BeLessThan(1);
        }

        [Fact]
        public void RoundTrip_ComplexObject_PreservesValues()
        {
            var original = new ComplexObject
            {
                Name = "Test Object",
                Value = 999,
                IsActive = true,
                Tags = new[] { "important", "urgent" },
                CreatedAt = DateTime.UtcNow
            };
            
            var dbValue = _converter.ConvertToDatabase(original, typeof(ComplexObject));
            var restored = _converter.ConvertFromDatabase(dbValue, typeof(ComplexObject));
            
            restored.Should().BeOfType<ComplexObject>();
            var restoredObj = (ComplexObject)restored!;
            
            restoredObj.Name.Should().Be(original.Name);
            restoredObj.Value.Should().Be(original.Value);
            restoredObj.IsActive.Should().Be(original.IsActive);
            restoredObj.Tags.Should().BeEquivalentTo(original.Tags);
            (original.CreatedAt - restoredObj.CreatedAt).TotalSeconds.Should().BeLessThan(1);
        }

        private enum TestEnum
        {
            Active = 1,
            Pending = 2,
            Inactive = 3
        }

        private class TestClass
        {
            public TestEnum Status { get; set; }

            [Property("status_int", Flags.None)]
            public TestEnum StatusAsInt { get; set; }
        }

        private class ComplexObject
        {
            public string Name { get; set; } = string.Empty;
            public int Value { get; set; }
            public bool IsActive { get; set; }
            public string[]? Tags { get; set; }
            public DateTime CreatedAt { get; set; }
        }
    }
}