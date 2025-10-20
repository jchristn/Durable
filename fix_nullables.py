#!/usr/bin/env python3
"""
Script to fix nullable warnings in Test.MySql C# files.
Adds nullable types (?) and Assert.NotNull() checks after assignments.
"""

import re
import sys

def fix_read_by_id_pattern(content):
    """Fix pattern: Type var = await repository.ReadByIdAsync(...)"""
    # Pattern: EntityType variable = await repository.ReadByIdAsync(id);
    # Replace with: EntityType? variable = await repository.ReadByIdAsync(id);
    #               Assert.NotNull(variable);

    pattern = r'(\s+)(\w+)\s+(\w+)\s*=\s*await\s+\w+\.ReadByIdAsync\([^)]+\);'

    def replacer(match):
        indent = match.group(1)
        type_name = match.group(2)
        var_name = match.group(3)
        return f'{indent}{type_name}? {var_name} = await {match.group(0).split("=")[1].strip()}\n{indent}Assert.NotNull({var_name});'

    return re.sub(pattern, replacer, content)

def fix_first_or_default_pattern(content):
    """Fix pattern: Type var = collection.FirstOrDefault()"""
    pattern = r'(\s+)(\w+)\s+(\w+)\s*=\s*([^;]+\.FirstOrDefault\([^)]*\));'

    def replacer(match):
        indent = match.group(1)
        type_name = match.group(2)
        var_name = match.group(3)
        rhs = match.group(4)
        return f'{indent}{type_name}? {var_name} = {rhs};\n{indent}Assert.NotNull({var_name});'

    return re.sub(pattern, replacer, content)

def process_file(filepath):
    """Process a single file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        original = content

        # Apply fixes
        content = fix_read_by_id_pattern(content)
        content = fix_first_or_default_pattern(content)

        # Only write if changes were made
        if content != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Fixed: {filepath}")
            return True
        else:
            print(f"No changes needed: {filepath}")
            return False

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return False

if __name__ == "__main__":
    files = [
        r"C:\Code\Durable\src\Test.MySql\MySqlBatchInsertTests.cs",
        r"C:\Code\Durable\src\Test.MySql\MySqlIntegrationTests.cs",
    ]

    for file in files:
        process_file(file)
