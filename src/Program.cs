using Durable;
using Durable.Sqlite;
using Test.Shared;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Configure controllers with basic model validation
builder.Services.AddControllers();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<IRepository<Author>>(provider =>
    new SqliteRepository<Author>("Data Source=test.db"));
builder.Services.AddSingleton<IRepository<Book>>(provider =>
    new SqliteRepository<Book>("Data Source=test.db"));
builder.Services.AddSingleton<IRepository<Company>>(provider =>
    new SqliteRepository<Company>("Data Source=test.db"));
builder.Services.AddSingleton<IRepository<Category>>(provider =>
    new SqliteRepository<Category>("Data Source=test.db"));
builder.Services.AddSingleton<IRepository<Person>>(provider =>
    new SqliteRepository<Person>("Data Source=test.db"));
builder.Services.AddSingleton<IRepository<AuthorCategory>>(provider =>
    new SqliteRepository<AuthorCategory>("Data Source=test.db"));

var app = builder.Build();

// Initialize database tables
await InitializeDatabase(app.Services);

// Enable Swagger for all environments
app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "Test API V1");
    c.RoutePrefix = "swagger";
});

app.MapControllers();

app.Run();

async Task InitializeDatabase(IServiceProvider services)
{
    var authorRepo = services.GetRequiredService<IRepository<Author>>();
    var bookRepo = services.GetRequiredService<IRepository<Book>>();
    var companyRepo = services.GetRequiredService<IRepository<Company>>();
    var categoryRepo = services.GetRequiredService<IRepository<Category>>();
    var personRepo = services.GetRequiredService<IRepository<Person>>();

    // Create tables in correct order (considering foreign key dependencies)
    
    // 1. Independent tables first
    await authorRepo.ExecuteSqlAsync(@"
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            industry TEXT
        )");

    await categoryRepo.ExecuteSqlAsync(@"
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT
        )");

    await personRepo.ExecuteSqlAsync(@"
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first TEXT NOT NULL,
            last TEXT NOT NULL,
            age INTEGER NOT NULL,
            email TEXT,
            salary DECIMAL NOT NULL,
            department TEXT
        )");

    // 2. Tables with foreign keys
    await authorRepo.ExecuteSqlAsync(@"
        CREATE TABLE IF NOT EXISTS authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            company_id INTEGER,
            FOREIGN KEY (company_id) REFERENCES companies(id)
        )");

    await bookRepo.ExecuteSqlAsync(@"
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            publisher_id INTEGER,
            FOREIGN KEY (author_id) REFERENCES authors(id),
            FOREIGN KEY (publisher_id) REFERENCES companies(id)
        )");

    // 3. Junction tables (many-to-many relationships)
    await authorRepo.ExecuteSqlAsync(@"
        CREATE TABLE IF NOT EXISTS author_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            FOREIGN KEY (author_id) REFERENCES authors(id),
            FOREIGN KEY (category_id) REFERENCES categories(id),
            UNIQUE(author_id, category_id)
        )");

    // Seed some sample data for testing
    await SeedSampleData(services);
}

async Task SeedSampleData(IServiceProvider services)
{
    var companyRepo = services.GetRequiredService<IRepository<Company>>();
    var authorRepo = services.GetRequiredService<IRepository<Author>>();
    var bookRepo = services.GetRequiredService<IRepository<Book>>();
    var categoryRepo = services.GetRequiredService<IRepository<Category>>();
    var personRepo = services.GetRequiredService<IRepository<Person>>();

    // Check if data already exists
    var existingCompanies = await companyRepo.CountAsync();
    if (existingCompanies > 0) return; // Data already seeded

    // Insert sample companies
    await companyRepo.ExecuteSqlAsync(@"
        INSERT INTO companies (name, industry) VALUES 
        ('Penguin Random House', 'Publishing'),
        ('HarperCollins', 'Publishing'),
        ('Simon & Schuster', 'Publishing')");

    // Insert sample categories
    await categoryRepo.ExecuteSqlAsync(@"
        INSERT INTO categories (name, description) VALUES 
        ('Fiction', 'Fictional stories and novels'),
        ('Horror', 'Scary and suspenseful stories'),
        ('Fantasy', 'Magical and supernatural stories'),
        ('Thriller', 'Fast-paced suspense novels'),
        ('Romance', 'Love stories and romantic novels')");

    // Insert sample authors
    await authorRepo.ExecuteSqlAsync(@"
        INSERT INTO authors (name, company_id) VALUES 
        ('Stephen King', 1),
        ('J.K. Rowling', 1),
        ('George R.R. Martin', 2),
        ('Agatha Christie', 3),
        ('John Grisham', 1)");

    // Insert sample books
    await bookRepo.ExecuteSqlAsync(@"
        INSERT INTO books (title, author_id, publisher_id) VALUES 
        ('The Shining', 1, 1),
        ('IT', 1, 1),
        ('Harry Potter and the Sorcerer''s Stone', 2, 1),
        ('A Game of Thrones', 3, 2),
        ('Murder on the Orient Express', 4, 3),
        ('The Firm', 5, 1),
        ('Pet Sematary', 1, 1)");

    // Insert sample author-category relationships
    await authorRepo.ExecuteSqlAsync(@"
        INSERT INTO author_categories (author_id, category_id) VALUES 
        (1, 2), -- Stephen King -> Horror
        (1, 4), -- Stephen King -> Thriller
        (2, 3), -- J.K. Rowling -> Fantasy
        (3, 3), -- George R.R. Martin -> Fantasy
        (4, 4), -- Agatha Christie -> Thriller
        (5, 4)  -- John Grisham -> Thriller
    ");

    // Insert sample people
    await personRepo.ExecuteSqlAsync(@"
        INSERT INTO people (first, last, age, email, salary, department) VALUES 
        ('John', 'Smith', 35, 'john.smith@company.com', 75000, 'Engineering'),
        ('Jane', 'Doe', 28, 'jane.doe@company.com', 68000, 'Marketing'),
        ('Mike', 'Johnson', 42, 'mike.johnson@company.com', 95000, 'Engineering'),
        ('Sarah', 'Wilson', 31, 'sarah.wilson@company.com', 72000, 'HR'),
        ('David', 'Brown', 38, 'david.brown@company.com', 88000, 'Sales')");
}