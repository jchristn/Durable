using System;

namespace Test.DataTypeConverter
{
    class Program
    {
        static int Main(string[] args)
        {
            Console.WriteLine("Running DataTypeConverter Unit Tests...");
            Console.WriteLine("=" + new string('=', 50));
            
            // Run tests manually
            var converter = new Durable.DataTypeConverter();
            var tests = new DataTypeConverterTests();
            var edgeCases = new EdgeCaseTests();
            int failures = 0;
            
            try
            {
                RunTest("ConvertToDatabase_DateTime", () => tests.ConvertToDatabase_DateTime_ReturnsFormattedString());
                RunTest("ConvertFromDatabase_DateTime", () => tests.ConvertFromDatabase_DateTimeString_ReturnsDateTime());
                RunTest("ConvertToDatabase_DateTimeOffset", () => tests.ConvertToDatabase_DateTimeOffset_ReturnsFormattedStringWithOffset());
                RunTest("ConvertFromDatabase_DateTimeOffset", () => tests.ConvertFromDatabase_DateTimeOffsetString_ReturnsDateTimeOffset());
                RunTest("ConvertToDatabase_TimeSpan", () => tests.ConvertToDatabase_TimeSpan_ReturnsFormattedString());
                RunTest("ConvertFromDatabase_TimeSpan", () => tests.ConvertFromDatabase_TimeSpanString_ReturnsTimeSpan());
                RunTest("ConvertToDatabase_Guid", () => tests.ConvertToDatabase_Guid_ReturnsString());
                RunTest("ConvertFromDatabase_Guid", () => tests.ConvertFromDatabase_GuidString_ReturnsGuid());
                RunTest("ConvertToDatabase_EnumAsString", () => tests.ConvertToDatabase_EnumAsString_ReturnsStringValue());
                RunTest("ConvertToDatabase_EnumAsInt", () => tests.ConvertToDatabase_EnumAsInt_ReturnsIntValue());
                RunTest("ConvertFromDatabase_StringToEnum", () => tests.ConvertFromDatabase_StringToEnum_ReturnsEnumValue());
                RunTest("ConvertFromDatabase_IntToEnum", () => tests.ConvertFromDatabase_IntToEnum_ReturnsEnumValue());
                RunTest("ConvertToDatabase_Array", () => tests.ConvertToDatabase_Array_ReturnsJsonString());
                RunTest("ConvertFromDatabase_Array", () => tests.ConvertFromDatabase_JsonStringToArray_ReturnsArray());
                RunTest("ConvertToDatabase_List", () => tests.ConvertToDatabase_List_ReturnsJsonString());
                RunTest("ConvertFromDatabase_List", () => tests.ConvertFromDatabase_JsonStringToList_ReturnsList());
                RunTest("ConvertToDatabase_ComplexObject", () => tests.ConvertToDatabase_ComplexObject_ReturnsJsonString());
                RunTest("ConvertFromDatabase_ComplexObject", () => tests.ConvertFromDatabase_JsonStringToComplexObject_ReturnsObject());
                RunTest("ConvertToDatabase_Null", () => tests.ConvertToDatabase_NullValue_ReturnsDBNull());
                RunTest("ConvertFromDatabase_NullForNullable", () => tests.ConvertFromDatabase_NullForNullableType_ReturnsNull());
                RunTest("ConvertFromDatabase_NullForValueType", () => tests.ConvertFromDatabase_NullForValueType_ReturnsDefault());
                RunTest("ConvertFromDatabase_DBNull", () => tests.ConvertFromDatabase_DBNullForNullableType_ReturnsNull());
                RunTest("ConvertToDatabase_NullableInt", () => tests.ConvertToDatabase_NullableIntWithValue_ReturnsValue());
                RunTest("ConvertFromDatabase_NullableInt", () => tests.ConvertFromDatabase_ValueForNullableInt_ReturnsNullableInt());
                RunTest("GetDatabaseTypeString", () => tests.GetDatabaseTypeString_CommonTypes_ReturnsCorrectType());
                RunTest("GetDatabaseTypeString_Enum", () => tests.GetDatabaseTypeString_EnumWithoutAttribute_ReturnsText());
                RunTest("GetDatabaseTypeString_EnumWithAttribute", () => tests.GetDatabaseTypeString_EnumWithIntegerAttribute_ReturnsInteger());
                RunTest("CanConvert", () => tests.CanConvert_AlwaysReturnsTrue());
                RunTest("RoundTrip_DateTime", () => tests.RoundTrip_DateTime_PreservesValue());
                RunTest("RoundTrip_ComplexObject", () => tests.RoundTrip_ComplexObject_PreservesValues());
                
                Console.WriteLine("\n--- Running Edge Case Tests ---");
                RunTest("MalformedDateTime", () => edgeCases.ConvertFromDatabase_MalformedDateTimeString_FallsBackToParse());
                RunTest("InvalidGuid", () => edgeCases.ConvertFromDatabase_InvalidGuidString_ThrowsException());
                RunTest("InvalidEnum", () => edgeCases.ConvertFromDatabase_InvalidEnumString_ThrowsException());
                RunTest("InvalidJson", () => edgeCases.ConvertFromDatabase_InvalidJsonForArray_ThrowsException());
                RunTest("EmptyArray", () => edgeCases.ConvertToDatabase_EmptyArray_ReturnsEmptyJsonArray());
                RunTest("EmptyJsonToArray", () => edgeCases.ConvertFromDatabase_EmptyJsonArray_ReturnsEmptyArray());
                RunTest("EmptyList", () => edgeCases.ConvertToDatabase_EmptyList_ReturnsEmptyJsonArray());
                RunTest("EmptyJsonToList", () => edgeCases.ConvertFromDatabase_EmptyJsonArray_ReturnsEmptyList());
                RunTest("Dictionary", () => edgeCases.ConvertToDatabase_DictionaryAsComplexObject_ReturnsJsonString());
                RunTest("JsonToDictionary", () => edgeCases.ConvertFromDatabase_JsonStringToDictionary_ReturnsDictionary());
                RunTest("TimeSpanZero", () => edgeCases.ConvertToDatabase_TimeSpanZero_ReturnsCorrectString());
                RunTest("TimeSpanWithDays", () => edgeCases.ConvertToDatabase_TimeSpanWithDays_ReturnsCorrectString());
                RunTest("LargeTimeSpan", () => edgeCases.ConvertFromDatabase_LargeTimeSpanString_ReturnsTimeSpan());
                RunTest("DateTimeMinValue", () => edgeCases.ConvertToDatabase_DateTimeMinValue_HandlesCorrectly());
                RunTest("DateTimeMaxValue", () => edgeCases.ConvertToDatabase_DateTimeMaxValue_HandlesCorrectly());
                RunTest("DateTimeAsNumber", () => edgeCases.ConvertFromDatabase_DateTimeAsNumber_ThrowsException());
                RunTest("NestedObject", () => edgeCases.ConvertToDatabase_NestedComplexObject_ReturnsJsonString());
                RunTest("NestedJson", () => edgeCases.ConvertFromDatabase_NestedJsonString_ReturnsNestedObject());
                RunTest("CircularReference", () => edgeCases.ConvertToDatabase_CircularReference_ShouldNotThrow());
                RunTest("NullableTypeStrings", () => edgeCases.GetDatabaseTypeString_NullableTypes_ReturnsCorrectType());
                RunTest("DateTimeFromDateTimeOffset", () => edgeCases.ConvertFromDatabase_DateTimeFromDateTimeOffset_ConvertsCorrectly());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nUnexpected error: {ex.Message}");
                failures++;
            }
            
            Console.WriteLine("=" + new string('=', 50));
            if (failures == 0)
            {
                Console.WriteLine("✅ ALL TESTS PASSED!");
            }
            else
            {
                Console.WriteLine($"❌ {failures} TESTS FAILED");
            }
            
            return failures;
            
            void RunTest(string name, Action test)
            {
                try
                {
                    test();
                    Console.WriteLine($"✓ {name}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ {name}: {ex.Message}");
                    failures++;
                }
            }
        }
    }
}