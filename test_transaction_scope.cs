using Test.Sqlite;
using Test.Shared;
using System;

class QuickTransactionTest
{
    static void Main()
    {
        Console.WriteLine("Running Transaction Scope Test...");
        try
        {
            var test = new TransactionScopeTest();
            test.TestConcurrentTransactionScopes();
            Console.WriteLine("✅ Transaction Scope Test PASSED!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Transaction Scope Test FAILED: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }
}