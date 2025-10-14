# MySQL Full-Text Search with Durable ORM

Durable ORM now supports MySQL's native full-text search capabilities through extension methods. This feature allows you to leverage MySQL's `MATCH() AGAINST()` syntax for efficient text searching.

## Prerequisites

Before using full-text search, you must create a `FULLTEXT` index on the columns you want to search:

```sql
-- Create a FULLTEXT index on a single column
CREATE FULLTEXT INDEX idx_description ON products(description);

-- Create a FULLTEXT index on multiple columns
CREATE FULLTEXT INDEX idx_title_description ON products(title, description);
```

## Search Modes

Four search modes are available through the `FullTextSearchMode` enum:

- **Natural**: Default natural language search. Interprets search terms as human language phrases.
- **NaturalWithQueryExpansion**: Performs natural search, then expands results with related documents.
- **Boolean**: Supports special operators (+, -, *, "", <, >, (), ~) for advanced search control.
- **BooleanWithQueryExpansion**: Combines boolean operators with query expansion.

## Usage Examples

### Basic Single-Column Search

Search a single text column using natural language mode:

```csharp
using Durable.MySql;

// Natural language search (default)
var results = await productRepo.Query()
    .WhereFullTextMatch(p => p.Description, "high quality durable")
    .ExecuteAsync();
```

### Boolean Mode Search

Use boolean operators for advanced control:

```csharp
// Boolean search with operators:
// + (must contain), - (must not contain), * (wildcard)
var results = await productRepo.Query()
    .WhereFullTextMatch(
        p => p.Description,
        "+quality -cheap durability*",
        FullTextSearchMode.Boolean
    )
    .ExecuteAsync();
```

### Multi-Column Search

Search across multiple columns (all must be part of the same FULLTEXT index):

```csharp
// Search both title and description
var results = await productRepo.Query()
    .WhereFullTextMatch(
        new Expression<Func<Product, object>>[]
        {
            p => p.Title,
            p => p.Description
        },
        "laptop computer",
        FullTextSearchMode.Natural
    )
    .ExecuteAsync();
```

### Query Expansion

Find related documents even without exact matches:

```csharp
// Query expansion helps find semantically related content
var results = await productRepo.Query()
    .WhereFullTextMatch(
        p => p.Description,
        "database",
        FullTextSearchMode.NaturalWithQueryExpansion
    )
    .ExecuteAsync();
```

### Combining with Other Query Operations

Full-text search integrates seamlessly with other query builder methods:

```csharp
// Combine full-text search with filters, sorting, and pagination
var results = await productRepo.Query()
    .WhereFullTextMatch(p => p.Description, "laptop computer")
    .Where(p => p.Price < 1000)
    .Where(p => p.IsAvailable == true)
    .OrderByDescending(p => p.CreatedDate)
    .Take(10)
    .ExecuteAsync();
```

## Boolean Search Operators

When using `FullTextSearchMode.Boolean`, you can use these operators:

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Word must be present | `+laptop +gaming` |
| `-` | Word must not be present | `laptop -refurbished` |
| `*` | Wildcard (trailing only) | `comp*` matches "computer", "compact" |
| `""` | Exact phrase | `"gaming laptop"` |
| `>` | Increase word relevance | `>laptop computer` |
| `<` | Decrease word relevance | `<laptop computer` |
| `()` | Group expressions | `+(laptop computer) -refurbished` |
| `~` | Negation/relevance reduction | `laptop ~gaming` |

## Generated SQL

The extension methods generate proper MySQL MATCH() AGAINST() syntax:

```csharp
// This code:
.WhereFullTextMatch(p => p.Description, "laptop", FullTextSearchMode.Natural)

// Generates this SQL:
MATCH(`description`) AGAINST('laptop' IN NATURAL LANGUAGE MODE)
```

## Important Notes

1. **FULLTEXT Index Required**: Columns must have a FULLTEXT index or queries will fail with a MySQL error.

2. **Minimum Word Length**: By default, MySQL ignores words shorter than 4 characters. Configure `ft_min_word_len` to change this.

3. **Stop Words**: Common words (the, is, at, etc.) are ignored. Configure `ft_stopword_file` to customize.

4. **InnoDB vs MyISAM**: Both storage engines support FULLTEXT indexes (MySQL 5.6+).

5. **Column Requirements**: All columns in a multi-column search must be part of the same FULLTEXT index.

6. **String Escaping**: Single quotes in search terms are automatically escaped.

## Performance Tips

- Create FULLTEXT indexes on columns you frequently search
- Use Boolean mode for precise control over search behavior
- Consider query expansion for broader result sets
- Monitor query performance with `EXPLAIN` for large datasets
- Adjust `ft_min_word_len` and `ft_max_word_len` based on your data

## Complete Example

```csharp
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Durable;
using Durable.MySql;

namespace Example
{
    [Entity("products")]
    public class Product
    {
        [Property("id", Flags.PrimaryKey | Flags.AutoIncrement)]
        public int Id { get; set; }

        [Property("title", Flags.String, 200)]
        public string Title { get; set; } = string.Empty;

        [Property("description", Flags.String, 2000)]
        public string Description { get; set; } = string.Empty;

        [Property("price")]
        public decimal Price { get; set; }

        [Property("is_available")]
        public bool IsAvailable { get; set; }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            string connectionString = "Server=localhost;Database=mydb;User=root;Password=pass;";
            var productRepo = new MySqlRepository<Product>(connectionString);

            // Search for laptops under $1000
            var affordableLaptops = await productRepo.Query()
                .WhereFullTextMatch(p => p.Description, "+laptop -refurbished", FullTextSearchMode.Boolean)
                .Where(p => p.Price < 1000)
                .Where(p => p.IsAvailable == true)
                .OrderByDescending(p => p.Price)
                .ExecuteAsync();

            foreach (var product in affordableLaptops)
            {
                Console.WriteLine($"{product.Title}: ${product.Price}");
            }
        }
    }
}
```

## Creating FULLTEXT Indexes in Entity Setup

```csharp
using MySqlConnector;

// Create table with FULLTEXT index
string createTableSql = @"
    CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(200),
        description TEXT,
        price DECIMAL(10,2),
        is_available BOOLEAN,
        FULLTEXT INDEX idx_description (description),
        FULLTEXT INDEX idx_title_description (title, description)
    ) ENGINE=InnoDB;
";

using (var connection = new MySqlConnection(connectionString))
{
    await connection.OpenAsync();
    using (var command = new MySqlCommand(createTableSql, connection))
    {
        await command.ExecuteNonQueryAsync();
    }
}
```
