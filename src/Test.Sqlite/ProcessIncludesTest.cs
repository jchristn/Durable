using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Durable.Sqlite;
using Test.Shared;

namespace Test.Sqlite
{
    public static class ProcessIncludesTest
    {
        public static void Main()
        {
            Console.WriteLine("ProcessIncludes Test Starting...");

            string databasePath = "process_includes_test.db";
            
            // Clean up existing database
            if (File.Exists(databasePath))
                File.Delete(databasePath);

            string connectionString = $"Data Source={databasePath}";

            try
            {
                RunIncludeTests(connectionString);
                Console.WriteLine("ProcessIncludes Test Completed Successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ProcessIncludes Test Failed: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                throw;
            }
            finally
            {
                // Clean up
                if (File.Exists(databasePath))
                    File.Delete(databasePath);
            }
        }

        private static void RunIncludeTests(string connectionString)
        {
            // Setup repositories
            var companyRepo = new SqliteRepository<Company>(connectionString);
            var authorRepo = new SqliteRepository<Author>(connectionString);
            var bookRepo = new SqliteRepository<Book>(connectionString);

            // Create tables
            CreateTables(connectionString);

            // Create test data
            var companies = CreateTestCompanies(companyRepo);
            var authors = CreateTestAuthors(authorRepo, companies);
            var books = CreateTestBooks(bookRepo, authors, companies);

            Console.WriteLine("Test data created successfully");

            // Test 1: Simple Include - Book with Author
            TestSimpleInclude(bookRepo);

            // Test 2: Nested Include - Book with Author and Author's Company
            TestNestedInclude(bookRepo);

            // Test 3: Multiple Includes - Book with both Author and Publisher
            TestMultipleIncludes(bookRepo);

            // Test 4: Include with Where clause
            TestIncludeWithWhere(bookRepo);

            Console.WriteLine("All tests passed!");
        }

        private static void CreateTables(string connectionString)
        {
            using var companyRepo = new SqliteRepository<Company>(connectionString);
            using var authorRepo = new SqliteRepository<Author>(connectionString);
            using var bookRepo = new SqliteRepository<Book>(connectionString);

            // Create tables SQL (simplified - normally you'd have a migration system)
            companyRepo.ExecuteSql(@"
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    industry TEXT
                )");

            authorRepo.ExecuteSql(@"
                CREATE TABLE IF NOT EXISTS authors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    company_id INTEGER,
                    FOREIGN KEY(company_id) REFERENCES companies(id)
                )");

            bookRepo.ExecuteSql(@"
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    publisher_id INTEGER,
                    FOREIGN KEY(author_id) REFERENCES authors(id),
                    FOREIGN KEY(publisher_id) REFERENCES companies(id)
                )");
        }

        private static List<Company> CreateTestCompanies(SqliteRepository<Company> repo)
        {
            var companies = new List<Company>
            {
                new Company { Name = "Penguin Random House", Industry = "Publishing" },
                new Company { Name = "HarperCollins", Industry = "Publishing" },
                new Company { Name = "Microsoft", Industry = "Technology" }
            };

            foreach (var company in companies)
            {
                repo.Create(company);
            }

            Console.WriteLine($"Created {companies.Count} companies");
            return companies;
        }

        private static List<Author> CreateTestAuthors(SqliteRepository<Author> repo, List<Company> companies)
        {
            var authors = new List<Author>
            {
                new Author { Name = "Stephen King", CompanyId = companies[0].Id },
                new Author { Name = "J.K. Rowling", CompanyId = companies[1].Id },
                new Author { Name = "Independent Author", CompanyId = null }
            };

            foreach (var author in authors)
            {
                repo.Create(author);
            }

            Console.WriteLine($"Created {authors.Count} authors");
            return authors;
        }

        private static List<Book> CreateTestBooks(SqliteRepository<Book> repo, List<Author> authors, List<Company> companies)
        {
            var books = new List<Book>
            {
                new Book { Title = "The Shining", AuthorId = authors[0].Id, PublisherId = companies[0].Id },
                new Book { Title = "IT", AuthorId = authors[0].Id, PublisherId = companies[0].Id },
                new Book { Title = "Harry Potter", AuthorId = authors[1].Id, PublisherId = companies[1].Id },
                new Book { Title = "Self Published Book", AuthorId = authors[2].Id, PublisherId = null }
            };

            foreach (var book in books)
            {
                repo.Create(book);
            }

            Console.WriteLine($"Created {books.Count} books");
            return books;
        }

        private static void TestSimpleInclude(SqliteRepository<Book> bookRepo)
        {
            Console.WriteLine("\n--- Test 1: Simple Include ---");

            var books = bookRepo.Query()
                .Include(b => b.Author)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books with authors");

            foreach (var book in books)
            {
                Console.WriteLine($"Book: {book.Title}");
                
                if (book.Author == null)
                {
                    throw new Exception($"Author should be loaded for book '{book.Title}' but was null");
                }

                Console.WriteLine($"  Author: {book.Author.Name}");
            }

            Console.WriteLine("✓ Simple Include test passed");
        }

        private static void TestNestedInclude(SqliteRepository<Book> bookRepo)
        {
            Console.WriteLine("\n--- Test 2: Nested Include ---");

            var books = bookRepo.Query()
                .Include(b => b.Author)
                .ThenInclude(a => a.Company)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books with authors and companies");

            foreach (var book in books)
            {
                Console.WriteLine($"Book: {book.Title}");
                
                if (book.Author == null)
                {
                    throw new Exception($"Author should be loaded for book '{book.Title}' but was null");
                }

                Console.WriteLine($"  Author: {book.Author.Name}");
                
                if (book.Author.CompanyId.HasValue)
                {
                    if (book.Author.Company == null)
                    {
                        throw new Exception($"Company should be loaded for author '{book.Author.Name}' but was null");
                    }
                    Console.WriteLine($"  Company: {book.Author.Company.Name}");
                }
                else
                {
                    Console.WriteLine($"  Company: None (Independent)");
                }
            }

            Console.WriteLine("✓ Nested Include test passed");
        }

        private static void TestMultipleIncludes(SqliteRepository<Book> bookRepo)
        {
            Console.WriteLine("\n--- Test 3: Multiple Includes ---");

            var books = bookRepo.Query()
                .Include(b => b.Author)
                .Include(b => b.Publisher)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} books with authors and publishers");

            foreach (var book in books)
            {
                Console.WriteLine($"Book: {book.Title}");
                
                if (book.Author == null)
                {
                    throw new Exception($"Author should be loaded for book '{book.Title}' but was null");
                }

                Console.WriteLine($"  Author: {book.Author.Name}");
                
                if (book.PublisherId.HasValue)
                {
                    if (book.Publisher == null)
                    {
                        throw new Exception($"Publisher should be loaded for book '{book.Title}' but was null");
                    }
                    Console.WriteLine($"  Publisher: {book.Publisher.Name}");
                }
                else
                {
                    Console.WriteLine($"  Publisher: None (Self Published)");
                }
            }

            Console.WriteLine("✓ Multiple Includes test passed");
        }

        private static void TestIncludeWithWhere(SqliteRepository<Book> bookRepo)
        {
            Console.WriteLine("\n--- Test 4: Include with Where ---");

            var books = bookRepo.Query()
                .Where(b => b.Title.Contains("Harry"))
                .Include(b => b.Author)
                .Include(b => b.Publisher)
                .Execute()
                .ToList();

            Console.WriteLine($"Retrieved {books.Count} Harry Potter books with includes");

            if (books.Count != 1)
            {
                throw new Exception($"Expected 1 Harry Potter book, got {books.Count}");
            }

            var book = books[0];
            
            if (book.Author == null)
            {
                throw new Exception("Author should be loaded but was null");
            }

            if (book.Publisher == null)
            {
                throw new Exception("Publisher should be loaded but was null");
            }

            Console.WriteLine($"Book: {book.Title}");
            Console.WriteLine($"  Author: {book.Author.Name}");
            Console.WriteLine($"  Publisher: {book.Publisher.Name}");

            Console.WriteLine("✓ Include with Where test passed");
        }
    }
}