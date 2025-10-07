namespace Test.SqlServer
{
    using Xunit;

    /// <summary>
    /// Collection definition for SQL Server data type tests.
    /// Tests in this collection will run sequentially to avoid table conflicts.
    /// </summary>
    [CollectionDefinition("SqlServerDataTypeTests", DisableParallelization = true)]
    public class SqlServerDataTypeTestsCollection
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
