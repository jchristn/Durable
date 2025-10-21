namespace Test.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Durable;
    using Durable.Sqlite;
    using Microsoft.Data.Sqlite;
    using Test.Shared;
    
    /// <summary>
    /// Tests include and join operations for related entity handling.
    /// </summary>
    public class IncludeTest
    {
        private static string _ConnectionString = "Data Source=include_test.db";
        private static SqliteConnectionFactory _ConnectionFactory;
        private static SqliteRepository<Book> _BookRepository;
        private static SqliteRepository<Author> _AuthorRepository;
        private static SqliteRepository<Company> _CompanyRepository;

        /// <summary>
        /// Executes include and join operation tests.
        /// </summary>
        public static void Run()
        {
            Console.WriteLine("=== Include/Join Test ===");
            Console.WriteLine();

            try
            {
                InitializeDatabase();
                SeedTestData();
                
                TestSimpleInclude();
                TestNestedInclude();
                TestMultipleIncludes();
                TestIncludeWithWhere();
                TestIncludeWithOrderBy();
                TestIncludeWithPaging();
                TestThenInclude();
                TestComplexQueryWithIncludes();
                
                Console.WriteLine("\nAll Include tests passed!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Test failed: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                CleanupDatabase();
            }
        }

        private static void InitializeDatabase()
        {
            Console.WriteLine("Initializing database...");
            
            // Delete existing database file
            if (System.IO.File.Exists("include_test.db"))
            {
                System.IO.File.Delete("include_test.db");
            }

            _ConnectionFactory = new SqliteConnectionFactory(_ConnectionString);
            _BookRepository = new SqliteRepository<Book>(_ConnectionFactory);
            _AuthorRepository = new SqliteRepository<Author>(_ConnectionFactory);
            _CompanyRepository = new SqliteRepository<Company>(_ConnectionFactory);

            using SqliteConnection connection = new SqliteConnection(_ConnectionString);
            connection.Open();

            // Create tables
            using SqliteCommand command = connection.CreateCommand();
            
            command.CommandText = @"
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    industry TEXT
                );";
            command.ExecuteNonQuery();

            command.CommandText = @"
                CREATE TABLE authors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    company_id INTEGER,
                    FOREIGN KEY (company_id) REFERENCES companies(id)
                );";
            command.ExecuteNonQuery();

            command.CommandText = @"
                CREATE TABLE books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    publisher_id INTEGER,
                    FOREIGN KEY (author_id) REFERENCES authors(id),
                    FOREIGN KEY (publisher_id) REFERENCES companies(id)
                );";
            command.ExecuteNonQuery();

            Console.WriteLine("Database initialized successfully.");
        }

        private static void SeedTestData()
        {
            Console.WriteLine("Seeding test data...");

            // Create companies
            Company techCorp = _CompanyRepository.Create(new Company { Name = "TechCorp Publishing" });
            Company literaryHouse = _CompanyRepository.Create(new Company { Name = "Literary House" });
            Company sciencePress = _CompanyRepository.Create(new Company { Name = "Science Press" });

            // Create authors
            Author johnDoe = _AuthorRepository.Create(new Author 
            { 
                Name = "John Doe", 
                CompanyId = techCorp.Id 
            });
            
            Author janeSmith = _AuthorRepository.Create(new Author 
            { 
                Name = "Jane Smith", 
                CompanyId = literaryHouse.Id 
            });
            
            Author bobJohnson = _AuthorRepository.Create(new Author 
            { 
                Name = "Bob Johnson", 
                CompanyId = null // No company
            });

            // Create books
            _BookRepository.Create(new Book 
            { 
                Title = "Introduction to C#", 
                AuthorId = johnDoe.Id,
                PublisherId = techCorp.Id
            });
            
            _BookRepository.Create(new Book 
            { 
                Title = "Advanced .NET", 
                AuthorId = johnDoe.Id,
                PublisherId = techCorp.Id
            });
            
            _BookRepository.Create(new Book 
            { 
                Title = "Poetry Collection", 
                AuthorId = janeSmith.Id,
                PublisherId = literaryHouse.Id
            });
            
            _BookRepository.Create(new Book 
            { 
                Title = "Science Fiction Novel", 
                AuthorId = bobJohnson.Id,
                PublisherId = sciencePress.Id
            });
            
            _BookRepository.Create(new Book 
            { 
                Title = "Independent Work", 
                AuthorId = bobJohnson.Id,
                PublisherId = null // Self-published
            });

            Console.WriteLine("Test data seeded successfully.");
        }

        private static void TestSimpleInclude()
        {
            Console.WriteLine("\n--- Test: Simple Include ---");
            
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books with authors");
            
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
            }

            // Verify that authors are loaded
            if (books.Any(b => b.Author == null && b.AuthorId != 0))
            {
                throw new Exception("Some books are missing their authors!");
            }

            Console.WriteLine("Simple Include test passed!");
        }

        private static void TestNestedInclude()
        {
            Console.WriteLine("\n--- Test: Nested Include (ThenInclude) ---");
            
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books with authors and companies");
            
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Author's Company: {book.Author?.Company?.Name ?? "null"}");
            }

            Console.WriteLine("Nested Include test passed!");
        }

        private static void TestMultipleIncludes()
        {
            Console.WriteLine("\n--- Test: Multiple Includes ---");
            
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .Include(b => b.Publisher)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books with authors and publishers");
            
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Publisher: {book.Publisher?.Name ?? "null"}");
            }

            Console.WriteLine("Multiple Includes test passed!");
        }

        private static void TestIncludeWithWhere()
        {
            Console.WriteLine("\n--- Test: Include with Where Clause ---");
            
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .Where(b => b.AuthorId == 1)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books by author ID 1");
            
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");
            }

            if (books.Any(b => b.Author == null))
            {
                throw new Exception("Author should be loaded for all books!");
            }

            Console.WriteLine("Include with Where test passed!");
        }

        private static void TestIncludeWithOrderBy()
        {
            Console.WriteLine("\n--- Test: Include with OrderBy ---");
            
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .OrderBy(b => b.Title)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books ordered by title");
            
            string previousTitle = "";
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");
                
                if (!string.IsNullOrEmpty(previousTitle) && 
                    string.Compare(previousTitle, book.Title, StringComparison.Ordinal) > 0)
                {
                    throw new Exception("Books are not properly ordered!");
                }
                previousTitle = book.Title;
            }

            Console.WriteLine("Include with OrderBy test passed!");
        }

        private static void TestIncludeWithPaging()
        {
            Console.WriteLine("\n--- Test: Include with Paging ---");
            
            List<Book> page1 = _BookRepository.Query()
                .Include(b => b.Author)
                .OrderBy(b => b.Id)
                .Take(2)
                .Execute()
                .ToList();

            Console.WriteLine($"Page 1: Retrieved {page1.Count} books");
            foreach (Book book in page1)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");
            }

            List<Book> page2 = _BookRepository.Query()
                .Include(b => b.Author)
                .OrderBy(b => b.Id)
                .Skip(2)
                .Take(2)
                .Execute()
                .ToList();

            Console.WriteLine($"Page 2: Retrieved {page2.Count} books");
            foreach (Book book in page2)
            {
                Console.WriteLine($"  Book: {book.Title} by {book.Author?.Name}");
            }

            // Verify no overlap
            if (page1.Any(p1 => page2.Any(p2 => p2.Id == p1.Id)))
            {
                throw new Exception("Paging resulted in duplicate records!");
            }

            Console.WriteLine("Include with Paging test passed!");
        }

        private static void TestThenInclude()
        {
            Console.WriteLine("\n--- Test: ThenInclude Chain ---");
            
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Include(b => b.Publisher)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books with full relationship chain");
            
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Author's Company: {book.Author?.Company?.Name ?? "null"}");
                Console.WriteLine($"    Publisher: {book.Publisher?.Name ?? "null"}");
            }

            Console.WriteLine("ThenInclude Chain test passed!");
        }

        private static void TestComplexQueryWithIncludes()
        {
            Console.WriteLine("\n--- Test: Complex Query with Includes ---");
            
            List<Book> books = _BookRepository.Query()
                .Include(b => b.Author)
                .ThenInclude<Author, Company>(a => a.Company)
                .Include(b => b.Publisher)
                .Where(b => b.PublisherId != null)
                .OrderByDescending(b => b.Id)
                .Take(3)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} published books (max 3) with all relationships");
            
            foreach (Book book in books)
            {
                Console.WriteLine($"  Book ID {book.Id}: {book.Title}");
                Console.WriteLine($"    Author: {book.Author?.Name ?? "null"}");
                Console.WriteLine($"    Author's Company: {book.Author?.Company?.Name ?? "null"}");
                Console.WriteLine($"    Publisher: {book.Publisher?.Name ?? "null"}");
                
                if (book.PublisherId == null)
                {
                    throw new Exception("Query should only return books with publishers!");
                }
            }

            Console.WriteLine("Complex Query test passed!");
        }

        private static void CleanupDatabase()
        {
            Console.WriteLine("\nCleaning up database...");

            // Dispose repositories and connection factory to release all connections
            _BookRepository?.Dispose();
            _AuthorRepository?.Dispose();
            _CompanyRepository?.Dispose();
            _ConnectionFactory?.Dispose();

            if (System.IO.File.Exists("include_test.db"))
            {
                System.IO.File.Delete("include_test.db");
            }

            Console.WriteLine("Database cleaned up.");
        }
    }
}