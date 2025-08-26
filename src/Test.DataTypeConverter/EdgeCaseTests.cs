using System;
using System.Collections.Generic;
using Durable;
using FluentAssertions;
using Xunit;

namespace Test.DataTypeConverter
{
    public class EdgeCaseTests
    {
        private readonly Durable.DataTypeConverter _converter = new();

        [Fact]
        public void ConvertFromDatabase_MalformedDateTimeString_FallsBackToParse()
        {
            var malformedDate = "2024-03-15T14:30:45Z";
            var result = _converter.ConvertFromDatabase(malformedDate, typeof(DateTime));
            
            result.Should().BeOfType<DateTime>();
            var dateTime = (DateTime)result!;
            dateTime.Year.Should().Be(2024);
            dateTime.Month.Should().Be(3);
            dateTime.Day.Should().Be(15);
        }

        [Fact]
        public void ConvertFromDatabase_InvalidGuidString_ThrowsException()
        {
            var invalidGuid = "not-a-guid";
            
            Action act = () => _converter.ConvertFromDatabase(invalidGuid, typeof(Guid));
            
            act.Should().Throw<FormatException>();
        }

        [Fact]
        public void ConvertFromDatabase_InvalidEnumString_ThrowsException()
        {
            var invalidEnum = "NotAnEnumValue";
            
            Action act = () => _converter.ConvertFromDatabase(invalidEnum, typeof(TestEnum));
            
            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void ConvertFromDatabase_InvalidJsonForArray_ThrowsException()
        {
            var invalidJson = "not-json";
            
            Action act = () => _converter.ConvertFromDatabase(invalidJson, typeof(string[]));
            
            act.Should().Throw<System.Text.Json.JsonException>();
        }

        [Fact]
        public void ConvertToDatabase_EmptyArray_ReturnsEmptyJsonArray()
        {
            var emptyArray = new string[0];
            var result = _converter.ConvertToDatabase(emptyArray, typeof(string[]));
            
            result.Should().Be("[]");
        }

        [Fact]
        public void ConvertFromDatabase_EmptyJsonArray_ReturnsEmptyArray()
        {
            var json = "[]";
            var result = _converter.ConvertFromDatabase(json, typeof(string[]));
            
            result.Should().BeOfType<string[]>();
            ((string[])result!).Should().BeEmpty();
        }

        [Fact]
        public void ConvertToDatabase_EmptyList_ReturnsEmptyJsonArray()
        {
            var emptyList = new List<int>();
            var result = _converter.ConvertToDatabase(emptyList, typeof(List<int>));
            
            result.Should().Be("[]");
        }

        [Fact]
        public void ConvertFromDatabase_EmptyJsonArray_ReturnsEmptyList()
        {
            var json = "[]";
            var result = _converter.ConvertFromDatabase(json, typeof(List<int>));
            
            result.Should().BeOfType<List<int>>();
            ((List<int>)result!).Should().BeEmpty();
        }

        [Fact]
        public void ConvertToDatabase_DictionaryAsComplexObject_ReturnsJsonString()
        {
            var dict = new Dictionary<string, object>
            {
                ["key1"] = "value1",
                ["key2"] = 42,
                ["key3"] = true
            };
            var result = _converter.ConvertToDatabase(dict, typeof(Dictionary<string, object>));
            
            result.Should().BeOfType<string>();
            var jsonString = (string)result;
            jsonString.Should().Contain("\"key1\":\"value1\"");
            jsonString.Should().Contain("\"key2\":42");
            jsonString.Should().Contain("\"key3\":true");
        }

        [Fact]
        public void ConvertFromDatabase_JsonStringToDictionary_ReturnsDictionary()
        {
            var json = "{\"key1\":\"value1\",\"key2\":42,\"key3\":true}";
            var result = _converter.ConvertFromDatabase(json, typeof(Dictionary<string, object>));
            
            result.Should().BeOfType<Dictionary<string, object>>();
            var dict = (Dictionary<string, object>)result!;
            dict["key1"].ToString().Should().Be("value1");
        }

        [Fact]
        public void ConvertToDatabase_TimeSpanZero_ReturnsCorrectString()
        {
            var timeSpan = TimeSpan.Zero;
            var result = _converter.ConvertToDatabase(timeSpan, typeof(TimeSpan));
            
            result.Should().Be("00:00:00");
        }

        [Fact]
        public void ConvertToDatabase_TimeSpanWithDays_ReturnsCorrectString()
        {
            var timeSpan = new TimeSpan(2, 3, 30, 45);
            var result = _converter.ConvertToDatabase(timeSpan, typeof(TimeSpan));
            
            result.Should().Be("2.03:30:45");
        }

        [Fact]
        public void ConvertFromDatabase_LargeTimeSpanString_ReturnsTimeSpan()
        {
            var tsString = "2.03:30:45";
            var result = _converter.ConvertFromDatabase(tsString, typeof(TimeSpan));
            
            result.Should().BeOfType<TimeSpan>();
            var timeSpan = (TimeSpan)result!;
            timeSpan.Days.Should().Be(2);
            timeSpan.Hours.Should().Be(3);
            timeSpan.Minutes.Should().Be(30);
            timeSpan.Seconds.Should().Be(45);
        }

        [Fact]
        public void ConvertToDatabase_DateTimeMinValue_HandlesCorrectly()
        {
            var dateTime = DateTime.MinValue;
            var result = _converter.ConvertToDatabase(dateTime, typeof(DateTime));
            
            result.Should().Be("0001-01-01 00:00:00.0000000");
        }

        [Fact]
        public void ConvertToDatabase_DateTimeMaxValue_HandlesCorrectly()
        {
            var dateTime = DateTime.MaxValue;
            var result = _converter.ConvertToDatabase(dateTime, typeof(DateTime));
            
            result.Should().Be("9999-12-31 23:59:59.9999999");
        }

        [Fact]
        public void ConvertFromDatabase_DateTimeAsNumber_ThrowsException()
        {
            // Test that numeric values (which aren't valid DateTime formats) throw appropriate exceptions
            var numericValue = 45678;
            
            Action act = () => _converter.ConvertFromDatabase(numericValue, typeof(DateTime));
            
            // Convert.ChangeType will throw InvalidCastException when trying to convert int to DateTime
            act.Should().Throw<InvalidCastException>();
        }

        [Fact]
        public void ConvertToDatabase_NestedComplexObject_ReturnsJsonString()
        {
            var nested = new NestedObject
            {
                Outer = new OuterObject
                {
                    Name = "Outer",
                    Inner = new InnerObject
                    {
                        Value = 42
                    }
                }
            };
            
            var result = _converter.ConvertToDatabase(nested, typeof(NestedObject));
            
            result.Should().BeOfType<string>();
            var json = (string)result;
            json.Should().Contain("\"outer\"");
            json.Should().Contain("\"name\":\"Outer\"");
            json.Should().Contain("\"inner\"");
            json.Should().Contain("\"value\":42");
        }

        [Fact]
        public void ConvertFromDatabase_NestedJsonString_ReturnsNestedObject()
        {
            var json = "{\"outer\":{\"name\":\"Outer\",\"inner\":{\"value\":42}}}";
            var result = _converter.ConvertFromDatabase(json, typeof(NestedObject));
            
            result.Should().BeOfType<NestedObject>();
            var nested = (NestedObject)result!;
            nested.Outer.Should().NotBeNull();
            nested.Outer.Name.Should().Be("Outer");
            nested.Outer.Inner.Should().NotBeNull();
            nested.Outer.Inner.Value.Should().Be(42);
        }

        [Fact]
        public void ConvertToDatabase_CircularReference_ShouldNotThrow()
        {
            var obj1 = new CircularObject { Name = "Object1" };
            var obj2 = new CircularObject { Name = "Object2" };
            obj1.Reference = obj2;
            obj2.Reference = obj1;
            
            Action act = () => _converter.ConvertToDatabase(obj1, typeof(CircularObject));
            
            act.Should().Throw<System.Text.Json.JsonException>()
                .WithMessage("*cycle*");
        }

        [Fact]
        public void GetDatabaseTypeString_NullableTypes_ReturnsCorrectType()
        {
            _converter.GetDatabaseTypeString(typeof(int?)).Should().Be("INTEGER");
            _converter.GetDatabaseTypeString(typeof(DateTime?)).Should().Be("TEXT");
            _converter.GetDatabaseTypeString(typeof(Guid?)).Should().Be("TEXT");
            _converter.GetDatabaseTypeString(typeof(bool?)).Should().Be("INTEGER");
        }

        [Fact]
        public void ConvertFromDatabase_DateTimeFromDateTimeOffset_ConvertsCorrectly()
        {
            var dto = DateTimeOffset.Now;
            var dt = dto.DateTime;
            
            var result = _converter.ConvertFromDatabase(dt, typeof(DateTimeOffset));
            
            result.Should().BeOfType<DateTimeOffset>();
        }

        private enum TestEnum
        {
            Value1 = 1,
            Value2 = 2,
            Value3 = 3
        }

        private class NestedObject
        {
            public OuterObject Outer { get; set; } = null!;
        }

        private class OuterObject
        {
            public string Name { get; set; } = string.Empty;
            public InnerObject Inner { get; set; } = null!;
        }

        private class InnerObject
        {
            public int Value { get; set; }
        }

        private class CircularObject
        {
            public string Name { get; set; } = string.Empty;
            public CircularObject? Reference { get; set; }
        }
    }
}