# PowerShell script to fix nullable warnings in Test.MySql project
# This script makes common fixes for CS8600, CS8602, CS8604, CS8625 warnings

$files = @(
    "C:\Code\Durable\src\Test.MySql\MySqlIntegrationTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlBatchInsertTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlConcurrencyIntegrationTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlEntityRelationshipTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlComplexExpressionTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlAdvancedQueryBuilderTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlGroupByTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlProjectionTests.cs",
    "C:\Code\Durable\src\Test.MySql\MySqlTransactionScopeTests.cs"
)

foreach ($file in $files) {
    if (Test-Path $file) {
        Write-Host "Processing $file..."

        $content = Get-Content $file -Raw

        # Pattern 1: Fix ReadByIdAsync calls
        # Before: EntityType entity = await repository.ReadByIdAsync(id);
        # After:  EntityType? entity = await repository.ReadByIdAsync(id);
        #         Assert.NotNull(entity);

        # Pattern 2: Fix ReadFirst/ReadMany calls
        # Before: EntityType entity = repository.Query()...Execute().FirstOrDefault();
        # After:  EntityType? entity = repository.Query()...Execute().FirstOrDefault();
        #         Assert.NotNull(entity);

        # This is complex to automate perfectly, so we'll use Claude to handle this
        Write-Host "File requires manual fixing by Claude: $file"
    }
}

Write-Host "Script complete. Files need manual review by Claude."
