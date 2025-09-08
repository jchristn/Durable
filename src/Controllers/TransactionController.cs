using Microsoft.AspNetCore.Mvc;
using Durable;
using Test.Shared;
using System.Diagnostics;

namespace TestApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TransactionController : ControllerBase
{
    private readonly IRepository<Author> _authorRepo;
    private readonly IRepository<Book> _bookRepo;
    private readonly IRepository<Person> _personRepo;
    private readonly IRepository<Company> _companyRepo;
    private readonly IRepository<Category> _categoryRepo;
    private readonly IRepository<AuthorCategory> _authorCategoryRepo;

    public TransactionController(
        IRepository<Author> authorRepo,
        IRepository<Book> bookRepo,
        IRepository<Person> personRepo,
        IRepository<Company> companyRepo,
        IRepository<Category> categoryRepo,
        IRepository<AuthorCategory> authorCategoryRepo)
    {
        _authorRepo = authorRepo;
        _bookRepo = bookRepo;
        _personRepo = personRepo;
        _companyRepo = companyRepo;
        _categoryRepo = categoryRepo;
        _authorCategoryRepo = authorCategoryRepo;
    }

    public class TransactionResult
    {
        public bool Success { get; set; }
        public string TransactionType { get; set; }
        public List<string> Operations { get; set; } = new List<string>();
        public long ExecutionTimeMs { get; set; }
        public string Message { get; set; }
        public object Data { get; set; }
    }

    public class BookTransferRequest
    {
        public int FromAuthorId { get; set; }
        public int ToAuthorId { get; set; }
        public List<int> BookIds { get; set; }
        public bool TransferAll { get; set; }
    }

    public class CompanyAcquisitionRequest
    {
        public int AcquiringCompanyId { get; set; }
        public int TargetCompanyId { get; set; }
        public bool MergeAuthors { get; set; }
        public bool DeleteTargetCompany { get; set; }
    }

    public class SalaryAdjustmentRequest
    {
        public string Department { get; set; }
        public decimal PercentageChange { get; set; }
        public decimal? MinimumNewSalary { get; set; }
        public string Reason { get; set; }
    }

    [HttpPost("transfer-books")]
    public async Task<IActionResult> TransferBooks([FromBody] BookTransferRequest request)
    {
        if (request == null)
            return BadRequest("Transfer request is required");

        if (request.FromAuthorId <= 0 || request.ToAuthorId <= 0)
            return BadRequest("Valid author IDs are required");

        if (request.FromAuthorId == request.ToAuthorId)
            return BadRequest("Source and target authors must be different");

        var stopwatch = Stopwatch.StartNew();
        var operations = new List<string>();

        // Start transaction
        var transaction = await _bookRepo.BeginTransactionAsync();
        
        try
        {
            // Verify both authors exist
            var fromAuthor = await _authorRepo.ReadByIdAsync(request.FromAuthorId, transaction);
            if (fromAuthor == null)
            {
                await transaction.RollbackAsync();
                return NotFound($"Source author with ID {request.FromAuthorId} not found");
            }
            operations.Add($"Verified source author: {fromAuthor.Name}");

            var toAuthor = await _authorRepo.ReadByIdAsync(request.ToAuthorId, transaction);
            if (toAuthor == null)
            {
                await transaction.RollbackAsync();
                return NotFound($"Target author with ID {request.ToAuthorId} not found");
            }
            operations.Add($"Verified target author: {toAuthor.Name}");

            // Get books to transfer
            List<Book> booksToTransfer;
            if (request.TransferAll)
            {
                booksToTransfer = await _bookRepo.ReadManyAsync(b => b.AuthorId == request.FromAuthorId, transaction)
                    .ToListAsync();
                operations.Add($"Found {booksToTransfer.Count} books to transfer");
            }
            else if (request.BookIds != null && request.BookIds.Any())
            {
                booksToTransfer = new List<Book>();
                foreach (var bookId in request.BookIds)
                {
                    var book = await _bookRepo.ReadByIdAsync(bookId, transaction);
                    if (book != null && book.AuthorId == request.FromAuthorId)
                        booksToTransfer.Add(book);
                }
                operations.Add($"Found {booksToTransfer.Count} of {request.BookIds.Count} requested books");
            }
            else
            {
                await transaction.RollbackAsync();
                return BadRequest("Either TransferAll must be true or BookIds must be provided");
            }

            if (!booksToTransfer.Any())
            {
                await transaction.RollbackAsync();
                return BadRequest("No books found to transfer");
            }

            // Transfer each book
            var transferredCount = 0;
            foreach (var book in booksToTransfer)
            {
                book.AuthorId = request.ToAuthorId;
                await _bookRepo.UpdateAsync(book, transaction);
                transferredCount++;
            }
            operations.Add($"Updated {transferredCount} books with new author ID");

            // Update author statistics (if needed)
            // In a real scenario, you might update counts, timestamps, etc.
            
            // Commit transaction
            await transaction.CommitAsync();
            operations.Add("Transaction committed successfully");

            stopwatch.Stop();

            var result = new TransactionResult
            {
                Success = true,
                TransactionType = "Book Transfer",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = $"Successfully transferred {transferredCount} books from '{fromAuthor.Name}' to '{toAuthor.Name}'",
                Data = new
                {
                    TransferredBooks = booksToTransfer.Select(b => new { b.Id, b.Title }),
                    FromAuthor = new { fromAuthor.Id, fromAuthor.Name },
                    ToAuthor = new { toAuthor.Id, toAuthor.Name }
                }
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            operations.Add($"Error occurred: {ex.Message}");
            operations.Add("Transaction rolled back");
            stopwatch.Stop();

            return StatusCode(500, new TransactionResult
            {
                Success = false,
                TransactionType = "Book Transfer",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = $"Transaction failed and was rolled back: {ex.Message}"
            });
        }
    }

    [HttpPost("company-acquisition")]
    public async Task<IActionResult> CompanyAcquisition([FromBody] CompanyAcquisitionRequest request)
    {
        if (request == null)
            return BadRequest("Acquisition request is required");

        if (request.AcquiringCompanyId <= 0 || request.TargetCompanyId <= 0)
            return BadRequest("Valid company IDs are required");

        if (request.AcquiringCompanyId == request.TargetCompanyId)
            return BadRequest("A company cannot acquire itself");

        var stopwatch = Stopwatch.StartNew();
        var operations = new List<string>();

        var transaction = await _companyRepo.BeginTransactionAsync();

        try
        {
            // Verify both companies exist
            var acquiringCompany = await _companyRepo.ReadByIdAsync(request.AcquiringCompanyId, transaction);
            if (acquiringCompany == null)
            {
                await transaction.RollbackAsync();
                return NotFound($"Acquiring company with ID {request.AcquiringCompanyId} not found");
            }
            operations.Add($"Acquiring company: {acquiringCompany.Name}");

            var targetCompany = await _companyRepo.ReadByIdAsync(request.TargetCompanyId, transaction);
            if (targetCompany == null)
            {
                await transaction.RollbackAsync();
                return NotFound($"Target company with ID {request.TargetCompanyId} not found");
            }
            operations.Add($"Target company: {targetCompany.Name}");

            // Transfer all authors from target to acquiring company
            if (request.MergeAuthors)
            {
                var authorsToTransfer = await _authorRepo.ReadManyAsync(
                    a => a.CompanyId == request.TargetCompanyId, transaction)
                    .ToListAsync();
                
                foreach (var author in authorsToTransfer)
                {
                    author.CompanyId = request.AcquiringCompanyId;
                    await _authorRepo.UpdateAsync(author, transaction);
                }
                operations.Add($"Transferred {authorsToTransfer.Count} authors to acquiring company");
            }

            // Transfer all books published by target company
            var booksToTransfer = await _bookRepo.ReadManyAsync(
                b => b.PublisherId == request.TargetCompanyId, transaction)
                .ToListAsync();
            
            foreach (var book in booksToTransfer)
            {
                book.PublisherId = request.AcquiringCompanyId;
                await _bookRepo.UpdateAsync(book, transaction);
            }
            operations.Add($"Transferred {booksToTransfer.Count} published books to acquiring company");

            // Delete or deactivate target company
            if (request.DeleteTargetCompany)
            {
                await _companyRepo.DeleteByIdAsync(request.TargetCompanyId, transaction);
                operations.Add("Deleted target company");
            }
            else
            {
                // In a real scenario, you might set an 'IsActive' flag to false
                targetCompany.Name = targetCompany.Name + " (Acquired by " + acquiringCompany.Name + ")";
                await _companyRepo.UpdateAsync(targetCompany, transaction);
                operations.Add("Updated target company name to reflect acquisition");
            }

            // Commit transaction
            await transaction.CommitAsync();
            operations.Add("Transaction committed successfully");

            stopwatch.Stop();

            var result = new TransactionResult
            {
                Success = true,
                TransactionType = "Company Acquisition",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = $"'{acquiringCompany.Name}' successfully acquired '{targetCompany.Name}'",
                Data = new
                {
                    AcquiringCompany = new { acquiringCompany.Id, acquiringCompany.Name },
                    TargetCompany = new { targetCompany.Id, targetCompany.Name },
                    AuthorsTransferred = request.MergeAuthors,
                    BooksTransferred = booksToTransfer.Count
                }
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            operations.Add($"Error occurred: {ex.Message}");
            operations.Add("Transaction rolled back");
            stopwatch.Stop();

            return StatusCode(500, new TransactionResult
            {
                Success = false,
                TransactionType = "Company Acquisition",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = $"Transaction failed and was rolled back: {ex.Message}"
            });
        }
    }

    [HttpPost("salary-adjustment")]
    public async Task<IActionResult> SalaryAdjustment([FromBody] SalaryAdjustmentRequest request)
    {
        if (request == null)
            return BadRequest("Salary adjustment request is required");

        if (string.IsNullOrWhiteSpace(request.Department))
            return BadRequest("Department is required");

        if (request.PercentageChange == 0)
            return BadRequest("Percentage change cannot be zero");

        var stopwatch = Stopwatch.StartNew();
        var operations = new List<string>();

        var transaction = await _personRepo.BeginTransactionAsync();

        try
        {
            // Get all people in the department
            var peopleInDept = await _personRepo.ReadManyAsync(
                p => p.Department == request.Department, transaction)
                .ToListAsync();

            if (!peopleInDept.Any())
            {
                await transaction.RollbackAsync();
                return NotFound($"No employees found in department: {request.Department}");
            }
            operations.Add($"Found {peopleInDept.Count} employees in {request.Department}");

            // Store original salaries for audit
            var salaryChanges = new List<object>();
            
            // Apply salary adjustment
            foreach (var person in peopleInDept)
            {
                var originalSalary = person.Salary;
                var newSalary = originalSalary * (1 + request.PercentageChange / 100);
                
                // Apply minimum salary if specified
                if (request.MinimumNewSalary.HasValue && newSalary < request.MinimumNewSalary.Value)
                {
                    newSalary = request.MinimumNewSalary.Value;
                }

                person.Salary = newSalary;
                await _personRepo.UpdateAsync(person, transaction);

                salaryChanges.Add(new
                {
                    PersonId = person.Id,
                    Name = $"{person.FirstName} {person.LastName}",
                    OriginalSalary = originalSalary,
                    NewSalary = newSalary,
                    ActualChange = Math.Round((newSalary - originalSalary) / originalSalary * 100, 2)
                });
            }
            operations.Add($"Updated salaries for {peopleInDept.Count} employees");

            // In a real scenario, you might also:
            // 1. Create audit records
            // 2. Send notifications
            // 3. Update budget tables
            // 4. Log the adjustment reason

            // Simulate audit log creation
            operations.Add($"Created audit log: '{request.Reason ?? "Salary adjustment"}'");

            // Commit transaction
            await transaction.CommitAsync();
            operations.Add("Transaction committed successfully");

            stopwatch.Stop();

            var result = new TransactionResult
            {
                Success = true,
                TransactionType = "Salary Adjustment",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = $"Successfully adjusted salaries for {peopleInDept.Count} employees in {request.Department}",
                Data = new
                {
                    Department = request.Department,
                    PercentageChange = request.PercentageChange,
                    MinimumSalary = request.MinimumNewSalary,
                    Reason = request.Reason,
                    Changes = salaryChanges
                }
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            operations.Add($"Error occurred: {ex.Message}");
            operations.Add("Transaction rolled back - all salaries remain unchanged");
            stopwatch.Stop();

            return StatusCode(500, new TransactionResult
            {
                Success = false,
                TransactionType = "Salary Adjustment",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = $"Transaction failed and was rolled back: {ex.Message}"
            });
        }
    }

    [HttpPost("rollback-demo")]
    public async Task<IActionResult> RollbackDemo([FromQuery] bool forceError = false)
    {
        var stopwatch = Stopwatch.StartNew();
        var operations = new List<string>();

        var transaction = await _authorRepo.BeginTransactionAsync();

        try
        {
            // Step 1: Create a new author
            var newAuthor = new Author
            {
                Name = "Transaction Test Author",
                CompanyId = 1
            };
            var createdAuthor = await _authorRepo.CreateAsync(newAuthor, transaction);
            operations.Add($"Created author with ID {createdAuthor.Id}");

            // Step 2: Create books for the author
            var book1 = new Book
            {
                Title = "Transaction Test Book 1",
                AuthorId = createdAuthor.Id,
                PublisherId = 1
            };
            await _bookRepo.CreateAsync(book1, transaction);
            operations.Add("Created first book");

            var book2 = new Book
            {
                Title = "Transaction Test Book 2",
                AuthorId = createdAuthor.Id,
                PublisherId = 1
            };
            await _bookRepo.CreateAsync(book2, transaction);
            operations.Add("Created second book");

            // Step 3: Force an error if requested
            if (forceError)
            {
                operations.Add("Forcing error as requested...");
                throw new InvalidOperationException("Intentional error to demonstrate rollback");
            }

            // Step 4: Create category association
            var categoryAssoc = new AuthorCategory
            {
                AuthorId = createdAuthor.Id,
                CategoryId = 1
            };
            await _authorCategoryRepo.CreateAsync(categoryAssoc, transaction);
            operations.Add("Created category association");

            // Commit if no error
            await transaction.CommitAsync();
            operations.Add("Transaction committed successfully");

            stopwatch.Stop();

            var result = new TransactionResult
            {
                Success = true,
                TransactionType = "Rollback Demonstration",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = "Transaction completed successfully - all changes committed",
                Data = new
                {
                    CreatedAuthorId = createdAuthor.Id,
                    Note = "Try with ?forceError=true to see rollback in action"
                }
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            operations.Add($"Error occurred: {ex.Message}");
            operations.Add("Transaction rolled back - no changes were saved");
            operations.Add("All operations (author, books, category) were undone");
            stopwatch.Stop();

            return Ok(new TransactionResult
            {
                Success = false,
                TransactionType = "Rollback Demonstration",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = "Transaction was rolled back - no data was saved to database",
                Data = new
                {
                    Note = "Even though we created an author and books, nothing was saved due to rollback",
                    ErrorMessage = ex.Message
                }
            });
        }
    }

    [HttpPost("complex-multi-step")]
    public async Task<IActionResult> ComplexMultiStep()
    {
        var stopwatch = Stopwatch.StartNew();
        var operations = new List<string>();

        var transaction = await _authorRepo.BeginTransactionAsync();

        try
        {
            // Simulate a complex business process with multiple steps
            operations.Add("Starting complex multi-step transaction...");

            // Step 1: Create a new company
            var newCompany = new Company
            {
                Name = "Transaction Publishing House",
                Industry = "Publishing"
            };
            var createdCompany = await _companyRepo.CreateAsync(newCompany, transaction);
            operations.Add($"Step 1: Created company '{createdCompany.Name}'");

            // Step 2: Create authors for the company
            var author1 = await _authorRepo.CreateAsync(new Author
            {
                Name = "Transaction Author One",
                CompanyId = createdCompany.Id
            }, transaction);
            
            var author2 = await _authorRepo.CreateAsync(new Author
            {
                Name = "Transaction Author Two",
                CompanyId = createdCompany.Id
            }, transaction);
            operations.Add($"Step 2: Created 2 authors for the company");

            // Step 3: Create books for each author
            var booksCreated = 0;
            for (int i = 1; i <= 3; i++)
            {
                await _bookRepo.CreateAsync(new Book
                {
                    Title = $"Author One Book {i}",
                    AuthorId = author1.Id,
                    PublisherId = createdCompany.Id
                }, transaction);
                booksCreated++;
            }

            for (int i = 1; i <= 2; i++)
            {
                await _bookRepo.CreateAsync(new Book
                {
                    Title = $"Author Two Book {i}",
                    AuthorId = author2.Id,
                    PublisherId = createdCompany.Id
                }, transaction);
                booksCreated++;
            }
            operations.Add($"Step 3: Created {booksCreated} books");

            // Step 4: Create categories and associate with authors
            var fictionCategory = await _categoryRepo.CreateAsync(new Category
            {
                Name = "Transaction Fiction",
                Description = "Fiction created in transaction"
            }, transaction);

            await _authorCategoryRepo.CreateAsync(new AuthorCategory
            {
                AuthorId = author1.Id,
                CategoryId = fictionCategory.Id
            }, transaction);

            await _authorCategoryRepo.CreateAsync(new AuthorCategory
            {
                AuthorId = author2.Id,
                CategoryId = fictionCategory.Id
            }, transaction);
            operations.Add("Step 4: Created category and associations");

            // Step 5: Verify everything was created correctly
            var companyAuthors = await _authorRepo.ReadManyAsync(
                a => a.CompanyId == createdCompany.Id, transaction).ToListAsync();
            
            if (companyAuthors.Count != 2)
            {
                throw new InvalidOperationException("Author count mismatch - rolling back");
            }
            operations.Add("Step 5: Verified data integrity");

            // Commit the entire transaction
            await transaction.CommitAsync();
            operations.Add("Transaction committed successfully");

            stopwatch.Stop();

            var result = new TransactionResult
            {
                Success = true,
                TransactionType = "Complex Multi-Step Transaction",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = "Successfully completed complex multi-step transaction",
                Data = new
                {
                    CompanyId = createdCompany.Id,
                    AuthorIds = new[] { author1.Id, author2.Id },
                    BooksCreated = booksCreated,
                    CategoryId = fictionCategory.Id,
                    Summary = "Created 1 company, 2 authors, 5 books, 1 category, and 2 associations in a single transaction"
                }
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            operations.Add($"Error in multi-step process: {ex.Message}");
            operations.Add("Entire transaction rolled back - no partial data saved");
            stopwatch.Stop();

            return StatusCode(500, new TransactionResult
            {
                Success = false,
                TransactionType = "Complex Multi-Step Transaction",
                Operations = operations,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds,
                Message = $"Complex transaction failed and was completely rolled back: {ex.Message}"
            });
        }
    }

    [HttpGet("transaction-info")]
    public IActionResult GetTransactionInfo()
    {
        return Ok(new
        {
            Description = "Transaction support in Durable ORM",
            Features = new[]
            {
                "ACID compliance - Atomicity, Consistency, Isolation, Durability",
                "All-or-nothing execution - Complete success or complete rollback",
                "Multi-table operations in single transaction",
                "Nested transaction support",
                "Automatic rollback on errors",
                "Transaction isolation levels"
            },
            AvailableEndpoints = new[]
            {
                new { Endpoint = "POST /api/transaction/transfer-books", Description = "Transfer books between authors atomically" },
                new { Endpoint = "POST /api/transaction/company-acquisition", Description = "Complex company merger with multiple table updates" },
                new { Endpoint = "POST /api/transaction/salary-adjustment", Description = "Bulk salary updates with audit trail" },
                new { Endpoint = "POST /api/transaction/rollback-demo?forceError=true", Description = "Demonstrates automatic rollback on error" },
                new { Endpoint = "POST /api/transaction/complex-multi-step", Description = "Multi-step business process in single transaction" }
            },
            UseCases = new[]
            {
                "Financial transactions requiring consistency",
                "Data migrations and bulk updates",
                "Complex business workflows",
                "Maintaining referential integrity",
                "Audit trail requirements"
            }
        });
    }
}