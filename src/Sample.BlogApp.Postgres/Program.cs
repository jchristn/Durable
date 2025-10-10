namespace Sample.BlogApp.Postgres
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Postgres;
    using Npgsql;

    /// <summary>
    /// Sample blog application demonstrating real-world usage of the Durable ORM with PostgreSQL.
    /// This application manages authors, blog posts, and comments with full CRUD operations.
    /// </summary>
    class Program
    {
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8602 // Dereference of a possibly null reference.

        #region Private-Members

        private static string _ConnectionString = "";
        private static string _Host = "";
        private static string _User = "";
        private static string _Password = "";
        private static string _Database = "";

        #endregion

        #region Public-Methods

        /// <summary>
        /// Builds the connection string from environment variables or prompts user.
        /// </summary>
        /// <param name="usePostgres">If true, connects to postgres database instead of blogapp.</param>
        /// <returns>A PostgreSQL connection string.</returns>
        private static string BuildConnectionString(bool usePostgres = false)
        {
            // Use cached values if already set, otherwise check environment variables
            if (string.IsNullOrEmpty(_Host))
            {
                _Host = Environment.GetEnvironmentVariable("POSTGRES_HOST") ?? "";
            }

            if (string.IsNullOrEmpty(_Database))
            {
                _Database = Environment.GetEnvironmentVariable("POSTGRES_DATABASE") ?? "";
            }

            if (string.IsNullOrEmpty(_User))
            {
                _User = Environment.GetEnvironmentVariable("POSTGRES_USER") ?? "";
            }

            if (string.IsNullOrEmpty(_Password))
            {
                _Password = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD") ?? "";
            }

            // If any required value is missing, prompt for all of them (only once)
            if (string.IsNullOrEmpty(_Host) || string.IsNullOrEmpty(_User) || string.IsNullOrEmpty(_Password))
            {
                Console.WriteLine("=== PostgreSQL Connection Setup ===");

                if (string.IsNullOrEmpty(_Host))
                {
                    Console.Write("Enter PostgreSQL host and port (e.g., 'localhost' or 'server.com:5432'): ");
                    Console.Write("(or press Enter for default 'localhost'): ");
                    _Host = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(_Host))
                    {
                        _Host = "localhost";
                    }
                }
                else
                {
                    Console.WriteLine($"Using host from environment: {_Host}");
                }

                if (string.IsNullOrEmpty(_User))
                {
                    Console.Write($"Enter PostgreSQL username (or press Enter for default '{Environment.UserName}'): ");
                    _User = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(_User))
                    {
                        _User = Environment.UserName;
                    }
                }
                else
                {
                    Console.WriteLine($"Using username from environment: {_User}");
                }

                if (string.IsNullOrEmpty(_Password))
                {
                    Console.Write("Enter PostgreSQL password (or press Enter if none): ");
                    _Password = Console.ReadLine() ?? "";
                }

                if (string.IsNullOrEmpty(_Database) && !usePostgres)
                {
                    Console.Write("Enter database name (or press Enter for default 'blogapp'): ");
                    _Database = Console.ReadLine() ?? "";
                    if (string.IsNullOrEmpty(_Database))
                    {
                        _Database = "blogapp";
                    }
                }
                else if (!usePostgres)
                {
                    Console.WriteLine($"Using database from environment: {_Database}");
                }

                Console.WriteLine();
            }

            // Determine which database to use
            string database;
            if (usePostgres)
            {
                database = "postgres";
            }
            else if (string.IsNullOrEmpty(_Database))
            {
                database = "blogapp";
            }
            else
            {
                database = _Database;
            }

            return $"Host={_Host};Database={database};Username={_User};Password={_Password};";
        }

        /// <summary>
        /// Masks the password in a connection string for safe display.
        /// </summary>
        /// <param name="connectionString">The connection string to mask.</param>
        /// <returns>Connection string with password hidden.</returns>
        private static string MaskConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return connectionString;

            int passwordIndex = connectionString.IndexOf("Password=", StringComparison.OrdinalIgnoreCase);
            if (passwordIndex == -1)
                return connectionString;

            int passwordStart = passwordIndex + "Password=".Length;
            int semicolonIndex = connectionString.IndexOf(';', passwordStart);

            if (semicolonIndex == -1)
                return connectionString.Substring(0, passwordStart) + "***";

            return connectionString.Substring(0, passwordStart) + "***" + connectionString.Substring(semicolonIndex);
        }

        /// <summary>
        /// Main entry point for the blog application.
        /// </summary>
        /// <param name="args">Command line arguments.</param>
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== Durable ORM Sample: Blog Application ===\n");

            // Parse command line arguments
            if (args.Length > 0)
            {
                _ConnectionString = args[0];
                Console.WriteLine($"Using connection string from command line: {MaskConnectionString(_ConnectionString)}\n");
            }
            else
            {
                _ConnectionString = BuildConnectionString();
                Console.WriteLine("Tip: You can specify a custom PostgreSQL connection string by passing it as an argument.");
                Console.WriteLine("     Example: dotnet Sample.BlogApp.Postgres.dll \"Host=localhost;Database=blogapp;Username=myuser;Password=mypass;\"\n");
            }

            try
            {
                InitializeDatabase();

                PostgresRepository<Author> authorRepo = new PostgresRepository<Author>(_ConnectionString);
                PostgresRepository<BlogPost> postRepo = new PostgresRepository<BlogPost>(_ConnectionString);
                PostgresRepository<Comment> commentRepo = new PostgresRepository<Comment>(_ConnectionString);

                await RunBlogScenarios(authorRepo, postRepo, commentRepo);

                Console.WriteLine("\n✅ Sample application completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ Application error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Initializes the PostgreSQL database and creates all required tables.
        /// </summary>
        private static void InitializeDatabase()
        {
            Console.WriteLine("Initializing database...");

            // First, connect to the default 'postgres' database to create our database
            string postgresConnectionString = BuildConnectionString(true);

            using (NpgsqlConnection connection = new NpgsqlConnection(postgresConnectionString))
            {
                connection.Open();

                // Check if database exists, create if not
                string checkDbSql = "SELECT 1 FROM pg_database WHERE datname = 'blogapp'";
                using (NpgsqlCommand checkCommand = new NpgsqlCommand(checkDbSql, connection))
                {
                    object result = checkCommand.ExecuteScalar();
                    if (result == null)
                    {
                        string createDatabaseSql = "CREATE DATABASE blogapp WITH ENCODING 'UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;";
                        using (NpgsqlCommand command = new NpgsqlCommand(createDatabaseSql, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                }
            }

            // Now connect to blogapp database to create tables
            using (NpgsqlConnection connection = new NpgsqlConnection(_ConnectionString))
            {
                connection.Open();

                string createAuthorsSql = @"
                    CREATE TABLE IF NOT EXISTS authors (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(50) NOT NULL UNIQUE,
                        email VARCHAR(100) NOT NULL UNIQUE,
                        full_name VARCHAR(100) NOT NULL,
                        bio VARCHAR(500),
                        joined_date TIMESTAMP NOT NULL,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE
                    );";

                string createBlogPostsSql = @"
                    CREATE TABLE IF NOT EXISTS blog_posts (
                        id SERIAL PRIMARY KEY,
                        author_id INTEGER NOT NULL,
                        title VARCHAR(200) NOT NULL,
                        slug VARCHAR(250) NOT NULL UNIQUE,
                        content TEXT NOT NULL,
                        excerpt VARCHAR(500),
                        is_published BOOLEAN NOT NULL DEFAULT FALSE,
                        view_count INTEGER NOT NULL DEFAULT 0,
                        created_date TIMESTAMP NOT NULL,
                        updated_date TIMESTAMP NOT NULL,
                        published_date TIMESTAMP NULL,
                        FOREIGN KEY (author_id) REFERENCES authors (id)
                    );";

                string createCommentsSql = @"
                    CREATE TABLE IF NOT EXISTS comments (
                        id SERIAL PRIMARY KEY,
                        post_id INTEGER NOT NULL,
                        commenter_name VARCHAR(100) NOT NULL,
                        commenter_email VARCHAR(100) NOT NULL,
                        content VARCHAR(1000) NOT NULL,
                        is_approved BOOLEAN NOT NULL DEFAULT FALSE,
                        created_date TIMESTAMP NOT NULL,
                        FOREIGN KEY (post_id) REFERENCES blog_posts (id)
                    );";

            using (NpgsqlCommand command = new NpgsqlCommand(createAuthorsSql, connection))
            {
                command.ExecuteNonQuery();
            }

            using (NpgsqlCommand command = new NpgsqlCommand(createBlogPostsSql, connection))
            {
                command.ExecuteNonQuery();
            }

            using (NpgsqlCommand command = new NpgsqlCommand(createCommentsSql, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        Console.WriteLine("✓ Database initialized\n");
        }

        /// <summary>
        /// Runs through various real-world blog scenarios to demonstrate ORM capabilities.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        /// <param name="commentRepo">The comment repository.</param>
        private static async Task RunBlogScenarios(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo,
            PostgresRepository<Comment> commentRepo)
        {
            await Scenario1_CreateAuthors(authorRepo);
            await Scenario2_CreateBlogPosts(authorRepo, postRepo);
            await Scenario3_AddComments(postRepo, commentRepo);
            await Scenario4_QueryAndFilter(authorRepo, postRepo, commentRepo);
            await Scenario5_UpdateOperations(postRepo, commentRepo);
            await Scenario6_Aggregations(authorRepo, postRepo, commentRepo);
            await Scenario7_Transactions(authorRepo, postRepo);
            await Scenario8_BatchOperations(postRepo);
            await Scenario9_AdvancedQueries(authorRepo, postRepo);
            await Scenario10_RawSqlQueries(authorRepo, postRepo, commentRepo);
            await Scenario11_PaginationAndStreaming(postRepo);
            await Scenario12_EdgeCases(authorRepo, postRepo, commentRepo);
        }

        /// <summary>
        /// Scenario 1: Create authors for the blog.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        private static async Task Scenario1_CreateAuthors(PostgresRepository<Author> authorRepo)
        {
            Console.WriteLine("=== Scenario 1: Creating Authors ===");

            int existingCount = await authorRepo.CountAsync();
            if (existingCount > 0)
            {
                Console.WriteLine($"Found {existingCount} existing authors, skipping creation.\n");
                return;
            }

            List<Author> authors = new List<Author>
            {
                new Author
                {
                    Username = "alice_tech",
                    Email = "alice@techblog.com",
                    FullName = "Alice Johnson",
                    Bio = "Senior software engineer passionate about distributed systems and cloud architecture.",
                    JoinedDate = DateTime.UtcNow.AddMonths(-6),
                    IsActive = true
                },
                new Author
                {
                    Username = "bob_data",
                    Email = "bob@datascience.com",
                    FullName = "Bob Smith",
                    Bio = "Data scientist exploring machine learning and AI applications.",
                    JoinedDate = DateTime.UtcNow.AddMonths(-3),
                    IsActive = true
                },
                new Author
                {
                    Username = "carol_dev",
                    Email = "carol@webdev.com",
                    FullName = "Carol Williams",
                    Bio = "Full-stack developer with a focus on modern web technologies.",
                    JoinedDate = DateTime.UtcNow.AddMonths(-1),
                    IsActive = true
                }
            };

            IEnumerable<Author> createdAuthors = await authorRepo.CreateManyAsync(authors);
            Console.WriteLine($"✓ Created {createdAuthors.Count()} authors");

            await foreach (Author author in authorRepo.ReadAllAsync())
            {
                Console.WriteLine($"  - {author}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 2: Create blog posts for authors.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        private static async Task Scenario2_CreateBlogPosts(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo)
        {
            Console.WriteLine("=== Scenario 2: Creating Blog Posts ===");

            int existingCount = await postRepo.CountAsync();
            if (existingCount > 0)
            {
                Console.WriteLine($"Found {existingCount} existing posts, skipping creation.\n");
                return;
            }

            Author alice = await authorRepo.ReadFirstAsync(a => a.Username == "alice_tech");
            Author bob = await authorRepo.ReadFirstAsync(a => a.Username == "bob_data");
            Author carol = await authorRepo.ReadFirstAsync(a => a.Username == "carol_dev");

            List<BlogPost> posts = new List<BlogPost>
            {
                new BlogPost
                {
                    AuthorId = alice.Id,
                    Title = "Introduction to Microservices Architecture",
                    Slug = "intro-to-microservices",
                    Content = "Microservices architecture is a design pattern that structures an application as a collection of loosely coupled services...",
                    Excerpt = "Learn the fundamentals of microservices architecture.",
                    IsPublished = true,
                    ViewCount = 0,
                    CreatedDate = DateTime.UtcNow.AddDays(-10),
                    UpdatedDate = DateTime.UtcNow.AddDays(-10),
                    PublishedDate = DateTime.UtcNow.AddDays(-10)
                },
                new BlogPost
                {
                    AuthorId = alice.Id,
                    Title = "Building Scalable APIs with REST",
                    Slug = "scalable-rest-apis",
                    Content = "RESTful APIs are the backbone of modern web applications. In this post, we'll explore best practices for building scalable REST APIs...",
                    Excerpt = "Best practices for building scalable REST APIs.",
                    IsPublished = true,
                    ViewCount = 0,
                    CreatedDate = DateTime.UtcNow.AddDays(-5),
                    UpdatedDate = DateTime.UtcNow.AddDays(-5),
                    PublishedDate = DateTime.UtcNow.AddDays(-5)
                },
                new BlogPost
                {
                    AuthorId = bob.Id,
                    Title = "Machine Learning Basics for Developers",
                    Slug = "ml-basics-developers",
                    Content = "Machine learning doesn't have to be intimidating. This guide introduces core concepts that every developer should know...",
                    Excerpt = "Core ML concepts every developer should know.",
                    IsPublished = true,
                    ViewCount = 0,
                    CreatedDate = DateTime.UtcNow.AddDays(-7),
                    UpdatedDate = DateTime.UtcNow.AddDays(-7),
                    PublishedDate = DateTime.UtcNow.AddDays(-7)
                },
                new BlogPost
                {
                    AuthorId = carol.Id,
                    Title = "Modern JavaScript Frameworks Comparison",
                    Slug = "js-frameworks-comparison",
                    Content = "React, Vue, Angular - which framework should you choose? Let's compare the pros and cons of each...",
                    Excerpt = "A detailed comparison of popular JavaScript frameworks.",
                    IsPublished = false,
                    ViewCount = 0,
                    CreatedDate = DateTime.UtcNow.AddDays(-2),
                    UpdatedDate = DateTime.UtcNow.AddDays(-1),
                    PublishedDate = null
                },
                new BlogPost
                {
                    AuthorId = carol.Id,
                    Title = "CSS Grid vs Flexbox: When to Use Each",
                    Slug = "css-grid-vs-flexbox",
                    Content = "CSS Grid and Flexbox are both powerful layout tools, but they excel in different scenarios...",
                    Excerpt = "Understanding when to use CSS Grid vs Flexbox.",
                    IsPublished = true,
                    ViewCount = 0,
                    CreatedDate = DateTime.UtcNow.AddDays(-3),
                    UpdatedDate = DateTime.UtcNow.AddDays(-3),
                    PublishedDate = DateTime.UtcNow.AddDays(-3)
                }
            };

            IEnumerable<BlogPost> createdPosts = await postRepo.CreateManyAsync(posts);
            Console.WriteLine($"✓ Created {createdPosts.Count()} blog posts");

            IEnumerable<BlogPost> publishedPosts = await postRepo.Query()
                .Where(p => p.IsPublished == true)
                .OrderByDescending(p => p.PublishedDate)
                .ExecuteAsync();

            Console.WriteLine($"  Published posts: {publishedPosts.Count()}");
            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 3: Add comments to blog posts.
        /// </summary>
        /// <param name="postRepo">The blog post repository.</param>
        /// <param name="commentRepo">The comment repository.</param>
        private static async Task Scenario3_AddComments(
            PostgresRepository<BlogPost> postRepo,
            PostgresRepository<Comment> commentRepo)
        {
            Console.WriteLine("=== Scenario 3: Adding Comments ===");

            int existingCount = await commentRepo.CountAsync();
            if (existingCount > 0)
            {
                Console.WriteLine($"Found {existingCount} existing comments, skipping creation.\n");
                return;
            }

            BlogPost microservicesPost = await postRepo.ReadFirstAsync(p => p.Slug == "intro-to-microservices");
            BlogPost mlPost = await postRepo.ReadFirstAsync(p => p.Slug == "ml-basics-developers");

            List<Comment> comments = new List<Comment>
            {
                new Comment
                {
                    PostId = microservicesPost.Id,
                    CommenterName = "John Doe",
                    CommenterEmail = "john@example.com",
                    Content = "Great article! Really helped me understand microservices better.",
                    IsApproved = true,
                    CreatedDate = DateTime.UtcNow.AddDays(-9)
                },
                new Comment
                {
                    PostId = microservicesPost.Id,
                    CommenterName = "Jane Smith",
                    CommenterEmail = "jane@example.com",
                    Content = "Could you elaborate more on service discovery patterns?",
                    IsApproved = true,
                    CreatedDate = DateTime.UtcNow.AddDays(-8)
                },
                new Comment
                {
                    PostId = mlPost.Id,
                    CommenterName = "Mike Johnson",
                    CommenterEmail = "mike@example.com",
                    Content = "This is spam! Buy our products now!",
                    IsApproved = false,
                    CreatedDate = DateTime.UtcNow.AddDays(-6)
                },
                new Comment
                {
                    PostId = mlPost.Id,
                    CommenterName = "Sarah Williams",
                    CommenterEmail = "sarah@example.com",
                    Content = "Excellent introduction to ML. Looking forward to more posts!",
                    IsApproved = true,
                    CreatedDate = DateTime.UtcNow.AddDays(-5)
                }
            };

            IEnumerable<Comment> createdComments = await commentRepo.CreateManyAsync(comments);
            Console.WriteLine($"✓ Created {createdComments.Count()} comments");

            int approvedCount = await commentRepo.CountAsync(c => c.IsApproved == true);
            int pendingCount = await commentRepo.CountAsync(c => c.IsApproved == false);

            Console.WriteLine($"  Approved: {approvedCount}, Pending: {pendingCount}");
            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 4: Query and filter data using LINQ expressions.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        /// <param name="commentRepo">The comment repository.</param>
        private static async Task Scenario4_QueryAndFilter(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo,
            PostgresRepository<Comment> commentRepo)
        {
            Console.WriteLine("=== Scenario 4: Querying and Filtering ===");

            Console.WriteLine("\n1. Find all published posts:");
            IEnumerable<BlogPost> publishedPosts = await postRepo.Query()
                .Where(p => p.IsPublished == true)
                .OrderByDescending(p => p.PublishedDate)
                .ExecuteAsync();

            foreach (BlogPost post in publishedPosts)
            {
                Console.WriteLine($"   - {post.Title} (Published: {post.PublishedDate:yyyy-MM-dd})");
            }

            Console.WriteLine("\n2. Find posts by a specific author:");
            Author alice = await authorRepo.ReadFirstAsync(a => a.Username == "alice_tech");
            IEnumerable<BlogPost> alicePosts = await postRepo.Query()
                .Where(p => p.AuthorId == alice.Id)
                .ExecuteAsync();

            Console.WriteLine($"   Alice has written {alicePosts.Count()} posts:");
            foreach (BlogPost post in alicePosts)
            {
                Console.WriteLine($"   - {post.Title}");
            }

            Console.WriteLine("\n3. Find approved comments for a specific post:");
            BlogPost microservicesPost = await postRepo.ReadFirstAsync(p => p.Slug == "intro-to-microservices");
            IEnumerable<Comment> approvedComments = await commentRepo.Query()
                .Where(c => c.PostId == microservicesPost.Id && c.IsApproved == true)
                .OrderBy(c => c.CreatedDate)
                .ExecuteAsync();

            Console.WriteLine($"   '{microservicesPost.Title}' has {approvedComments.Count()} approved comments:");
            foreach (Comment comment in approvedComments)
            {
                Console.WriteLine($"   - {comment.CommenterName}: \"{comment.Content}\"");
            }

            Console.WriteLine("\n4. Search posts by title (contains 'API'):");
            IEnumerable<BlogPost> apiPosts = await postRepo.Query()
                .Where(p => p.Title.Contains("API"))
                .ExecuteAsync();

            foreach (BlogPost post in apiPosts)
            {
                Console.WriteLine($"   - {post.Title}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 5: Update operations including single and batch updates.
        /// </summary>
        /// <param name="postRepo">The blog post repository.</param>
        /// <param name="commentRepo">The comment repository.</param>
        private static async Task Scenario5_UpdateOperations(
            PostgresRepository<BlogPost> postRepo,
            PostgresRepository<Comment> commentRepo)
        {
            Console.WriteLine("=== Scenario 5: Update Operations ===");

            Console.WriteLine("\n1. Increment view count for a post:");
            BlogPost microservicesPost = await postRepo.ReadFirstAsync(p => p.Slug == "intro-to-microservices");
            int originalViews = microservicesPost.ViewCount;
            microservicesPost.ViewCount += 10;
            microservicesPost.UpdatedDate = DateTime.UtcNow;
            await postRepo.UpdateAsync(microservicesPost);

            BlogPost updatedPost = await postRepo.ReadByIdAsync(microservicesPost.Id);
            Console.WriteLine($"   Views: {originalViews} → {updatedPost.ViewCount}");

            Console.WriteLine("\n2. Publish a draft post:");
            BlogPost draftPost = await postRepo.ReadFirstOrDefaultAsync(p => p.IsPublished == false);
            if (draftPost != null)
            {
                draftPost.IsPublished = true;
                draftPost.PublishedDate = DateTime.UtcNow;
                draftPost.UpdatedDate = DateTime.UtcNow;
                await postRepo.UpdateAsync(draftPost);
                Console.WriteLine($"   Published: '{draftPost.Title}'");
            }

            Console.WriteLine("\n3. Batch update: Approve all pending comments:");
            int approvedCount = await commentRepo.UpdateFieldAsync(
                c => c.IsApproved == false,
                c => c.IsApproved,
                true
            );
            Console.WriteLine($"   Approved {approvedCount} comments");

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 6: Demonstrate aggregation functions.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        /// <param name="commentRepo">The comment repository.</param>
        private static async Task Scenario6_Aggregations(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo,
            PostgresRepository<Comment> commentRepo)
        {
            Console.WriteLine("=== Scenario 6: Aggregations ===");

            int totalAuthors = await authorRepo.CountAsync();
            int activeAuthors = await authorRepo.CountAsync(a => a.IsActive == true);

            int totalPosts = await postRepo.CountAsync();
            int publishedPosts = await postRepo.CountAsync(p => p.IsPublished == true);

            int totalComments = await commentRepo.CountAsync();
            int approvedComments = await commentRepo.CountAsync(c => c.IsApproved == true);

            Console.WriteLine($"\n📊 Blog Statistics:");
            Console.WriteLine($"   Authors: {totalAuthors} ({activeAuthors} active)");
            Console.WriteLine($"   Posts: {totalPosts} ({publishedPosts} published)");
            Console.WriteLine($"   Comments: {totalComments} ({approvedComments} approved)");

            int maxViews = await postRepo.MaxAsync(p => p.ViewCount);
            BlogPost? mostViewed = await postRepo.ReadFirstAsync(p => p.ViewCount == maxViews);

            if (mostViewed != null)
            {
                Console.WriteLine($"\n🔥 Most viewed post: '{mostViewed.Title}' with {mostViewed.ViewCount} views");
            }
            else
            {
                Console.WriteLine($"\n🔥 Most viewed post: (no posts found with max views: {maxViews})");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 7: Demonstrate transaction usage for atomic operations.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        private static async Task Scenario7_Transactions(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo)
        {
            Console.WriteLine("=== Scenario 7: Transactions ===");

            Console.WriteLine("\n1. Creating author and post atomically:");

            bool authorExists = await authorRepo.ExistsAsync(a => a.Username == "dave_ops");
            if (authorExists)
            {
                Console.WriteLine("   Author 'dave_ops' already exists, skipping transaction demo");
            }
            else
            {
                using (ITransaction transaction = await authorRepo.BeginTransactionAsync())
                {
                    try
                    {
                        Author newAuthor = new Author
                        {
                            Username = "dave_ops",
                            Email = "dave@devops.com",
                            FullName = "Dave Wilson",
                            Bio = "DevOps engineer focused on CI/CD and infrastructure automation.",
                            JoinedDate = DateTime.UtcNow,
                            IsActive = true
                        };

                        Author createdAuthor = await authorRepo.CreateAsync(newAuthor, transaction);
                        Console.WriteLine($"   ✓ Created author: {createdAuthor.Username}");

                        BlogPost newPost = new BlogPost
                        {
                            AuthorId = createdAuthor.Id,
                            Title = "Introduction to Kubernetes",
                            Slug = "intro-to-kubernetes",
                            Content = "Kubernetes is a powerful container orchestration platform...",
                            Excerpt = "Get started with Kubernetes orchestration.",
                            IsPublished = true,
                            ViewCount = 0,
                            CreatedDate = DateTime.UtcNow,
                            UpdatedDate = DateTime.UtcNow,
                            PublishedDate = DateTime.UtcNow
                        };

                        BlogPost createdPost = await postRepo.CreateAsync(newPost, transaction);
                        Console.WriteLine($"   ✓ Created post: {createdPost.Title}");

                        await transaction.CommitAsync();
                        Console.WriteLine("   ✓ Transaction committed");
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                        Console.WriteLine($"   ✗ Transaction rolled back: {ex.Message}");
                    }
                }
            }

            Console.WriteLine("\n2. Testing rollback scenario:");
            int postCountBefore = await postRepo.CountAsync();

            using (ITransaction transaction = await postRepo.BeginTransactionAsync())
            {
                try
                {
                    // Get a valid author for the test
                    Author testAuthor = await authorRepo.ReadFirstAsync(a => a.Username == "alice_tech");

                    BlogPost tempPost = new BlogPost
                    {
                        AuthorId = testAuthor.Id,
                        Title = "This Post Will Be Rolled Back",
                        Slug = "rollback-test",
                        Content = "Temporary content",
                        Excerpt = "This will be rolled back",
                        IsPublished = false,
                        ViewCount = 0,
                        CreatedDate = DateTime.UtcNow,
                        UpdatedDate = DateTime.UtcNow
                    };

                    await postRepo.CreateAsync(tempPost, transaction);
                    Console.WriteLine("   Created temporary post");

                    await transaction.RollbackAsync();
                    Console.WriteLine("   ✓ Transaction rolled back");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"   Error: {ex.Message}");
                }
            }

            int postCountAfter = await postRepo.CountAsync();
            Console.WriteLine($"   Post count before: {postCountBefore}, after: {postCountAfter} (unchanged)");

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 8: Demonstrate batch operations for efficiency.
        /// </summary>
        /// <param name="postRepo">The blog post repository.</param>
        private static async Task Scenario8_BatchOperations(PostgresRepository<BlogPost> postRepo)
        {
            Console.WriteLine("=== Scenario 8: Batch Operations ===");

            Console.WriteLine("\n1. Batch update: Reset view counts for all posts:");
            int updatedCount = await postRepo.UpdateFieldAsync(
                p => p.ViewCount > 0,
                p => p.ViewCount,
                0
            );
            Console.WriteLine($"   Updated {updatedCount} posts");

            Console.WriteLine("\n2. Check for posts that need attention:");
            bool hasDrafts = await postRepo.ExistsAsync(p => p.IsPublished == false);
            Console.WriteLine($"   Has draft posts: {hasDrafts}");

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 9: Advanced query features including string operations, date filtering, and complex conditions.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        private static async Task Scenario9_AdvancedQueries(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo)
        {
            Console.WriteLine("=== Scenario 9: Advanced Query Features ===");

            Console.WriteLine("\n1. String operations (Contains):");
            IEnumerable<Author> techAuthors = await authorRepo.Query()
                .Where(a => a.Username.Contains("tech") || a.Username.Contains("data"))
                .ExecuteAsync();
            Console.WriteLine($"   Authors with tech/data in username: {techAuthors.Count()}");
            foreach (Author author in techAuthors)
            {
                Console.WriteLine($"     - {author.Username}");
            }

            Console.WriteLine("\n2. Date filtering (posts from last 30 days):");
            DateTime thirtyDaysAgo = DateTime.UtcNow.AddDays(-30);
            IEnumerable<BlogPost> recentPosts = await postRepo.Query()
                .Where(p => p.CreatedDate > thirtyDaysAgo)
                .OrderByDescending(p => p.CreatedDate)
                .ExecuteAsync();
            Console.WriteLine($"   Recent posts: {recentPosts.Count()}");

            Console.WriteLine("\n3. Complex conditions with AND/OR:");
            IEnumerable<BlogPost> popularOrRecent = await postRepo.Query()
                .Where(p => p.ViewCount > 5 || p.CreatedDate > thirtyDaysAgo)
                .Where(p => p.IsPublished == true)
                .ExecuteAsync();
            Console.WriteLine($"   Popular or recent published posts: {popularOrRecent.Count()}");

            Console.WriteLine("\n4. Case-insensitive search:");
            IEnumerable<BlogPost> apiPosts = await postRepo.Query()
                .Where(p => p.Title.ToLower().Contains("api") || p.Content.ToLower().Contains("api"))
                .ExecuteAsync();
            Console.WriteLine($"   Posts mentioning 'API': {apiPosts.Count()}");

            Console.WriteLine("\n5. Combining multiple OrderBy clauses:");
            IEnumerable<BlogPost> orderedPosts = await postRepo.Query()
                .Where(p => p.IsPublished == true)
                .OrderByDescending(p => p.ViewCount)
                .ThenByDescending(p => p.PublishedDate)
                .ThenBy(p => p.Title)
                .Take(5)
                .ExecuteAsync();
            Console.WriteLine($"   Top 5 posts by views, then date, then title:");
            foreach (BlogPost post in orderedPosts)
            {
                Console.WriteLine($"     - {post.Title} ({post.ViewCount} views)");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 10: Raw SQL queries for complex operations and custom result types.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        /// <param name="commentRepo">The comment repository.</param>
        private static async Task Scenario10_RawSqlQueries(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo,
            PostgresRepository<Comment> commentRepo)
        {
            Console.WriteLine("=== Scenario 10: Raw SQL Queries ===");

            Console.WriteLine("\n1. Complex join query with raw SQL:");
            string joinSql = @"
                SELECT
                    a.username as AuthorUsername,
                    COUNT(DISTINCT p.id) as TotalPosts,
                    COUNT(DISTINCT CASE WHEN p.is_published = TRUE THEN p.id END) as PublishedPosts,
                    COALESCE(SUM(p.view_count), 0) as TotalViews
                FROM authors a
                LEFT JOIN blog_posts p ON a.id = p.author_id
                GROUP BY a.id, a.username
                ORDER BY TotalViews DESC";

            List<BlogStatistics> stats = new List<BlogStatistics>();
            await foreach (BlogStatistics stat in authorRepo.FromSqlAsync<BlogStatistics>(joinSql))
            {
                stats.Add(stat);
            }

            Console.WriteLine($"   Author statistics:");
            foreach (BlogStatistics stat in stats)
            {
                Console.WriteLine($"     - {stat}");
            }

            Console.WriteLine("\n2. Custom aggregation with raw SQL:");
            string aggregateSql = @"
                SELECT
                    COUNT(*) as post_count,
                    AVG(view_count) as avg_views,
                    MAX(view_count) as max_views
                FROM blog_posts
                WHERE is_published = TRUE";

            int postCount = 0;
            await using (System.Data.Common.DbConnection connection = new Npgsql.NpgsqlConnection(authorRepo.Settings.BuildConnectionString()))
            {
                await connection.OpenAsync();
                await using (System.Data.Common.DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = aggregateSql;
                    await using (System.Data.Common.DbDataReader reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            postCount = reader.GetInt32(0);
                            double avgViews = reader.IsDBNull(1) ? 0 : reader.GetDouble(1);
                            int maxViews = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);
                            Console.WriteLine($"   Published Posts: {postCount}, Avg Views: {avgViews:F1}, Max Views: {maxViews}");
                        }
                    }
                }
            }

            Console.WriteLine("\n3. ExecuteSql for bulk operations:");
            int affectedRows = await postRepo.ExecuteSqlAsync(
                "UPDATE blog_posts SET excerpt = SUBSTRING(content, 1, 100) WHERE excerpt IS NULL OR excerpt = ''"
            );
            Console.WriteLine($"   Updated {affectedRows} posts with auto-generated excerpts");

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 11: Pagination and streaming patterns for large datasets.
        /// </summary>
        /// <param name="postRepo">The blog post repository.</param>
        private static async Task Scenario11_PaginationAndStreaming(PostgresRepository<BlogPost> postRepo)
        {
            Console.WriteLine("=== Scenario 11: Pagination and Streaming ===");

            Console.WriteLine("\n1. Paginated results (page 1 of 2, page size 3):");
            int pageSize = 3;
            int page = 1;

            IEnumerable<BlogPost> firstPage = await postRepo.Query()
                .Where(p => p.IsPublished == true)
                .OrderByDescending(p => p.PublishedDate)
                .Skip((page - 1) * pageSize)
                .Take(pageSize)
                .ExecuteAsync();

            Console.WriteLine($"   Page {page}:");
            foreach (BlogPost post in firstPage)
            {
                Console.WriteLine($"     - {post.Title}");
            }

            Console.WriteLine("\n2. Second page:");
            page = 2;
            IEnumerable<BlogPost> secondPage = await postRepo.Query()
                .Where(p => p.IsPublished == true)
                .OrderByDescending(p => p.PublishedDate)
                .Skip((page - 1) * pageSize)
                .Take(pageSize)
                .ExecuteAsync();

            Console.WriteLine($"   Page {page}:");
            foreach (BlogPost post in secondPage)
            {
                Console.WriteLine($"     - {post.Title}");
            }

            Console.WriteLine("\n3. Streaming large result sets with IAsyncEnumerable:");
            int streamedCount = 0;
            await foreach (BlogPost post in postRepo.ReadAllAsync())
            {
                streamedCount++;
                if (streamedCount <= 3)
                {
                    Console.WriteLine($"     Streamed: {post.Title}");
                }
            }
            Console.WriteLine($"   Total streamed: {streamedCount} posts (memory efficient!)");

            Console.WriteLine("\n4. Calculate total pages:");
            int totalPosts = await postRepo.CountAsync(p => p.IsPublished == true);
            int totalPages = (int)Math.Ceiling((double)totalPosts / pageSize);
            Console.WriteLine($"   Total posts: {totalPosts}, Page size: {pageSize}, Total pages: {totalPages}");

            Console.WriteLine();
        }

        /// <summary>
        /// Scenario 12: Edge cases and error handling patterns.
        /// </summary>
        /// <param name="authorRepo">The author repository.</param>
        /// <param name="postRepo">The blog post repository.</param>
        /// <param name="commentRepo">The comment repository.</param>
        private static async Task Scenario12_EdgeCases(
            PostgresRepository<Author> authorRepo,
            PostgresRepository<BlogPost> postRepo,
            PostgresRepository<Comment> commentRepo)
        {
            Console.WriteLine("=== Scenario 12: Edge Cases and Error Handling ===");

            Console.WriteLine("\n1. Handling non-existent records:");
            BlogPost nonExistent = await postRepo.ReadByIdAsync(99999);
            Console.WriteLine($"   ReadById(99999) returned: {(nonExistent == null ? "null" : "a post")}");

            Console.WriteLine("\n2. ReadFirstOrDefault with no matches:");
            BlogPost noMatch = await postRepo.ReadFirstOrDefaultAsync(p => p.ViewCount > 1000000);
            Console.WriteLine($"   ReadFirstOrDefault (no matches) returned: {(noMatch == null ? "null" : "a post")}");

            Console.WriteLine("\n3. Empty collection operations:");
            IEnumerable<BlogPost> emptyResult = await postRepo.Query()
                .Where(p => p.Title == "This Post Does Not Exist")
                .ExecuteAsync();
            Console.WriteLine($"   Query with no results returned: {emptyResult.Count()} posts");

            Console.WriteLine("\n4. Update non-existent record:");
            BlogPost fakePost = new BlogPost
            {
                Id = 99999,
                AuthorId = 1,
                Title = "Fake Post",
                Slug = "fake",
                Content = "Fake",
                Excerpt = "Fake",
                IsPublished = false,
                ViewCount = 0,
                CreatedDate = DateTime.UtcNow,
                UpdatedDate = DateTime.UtcNow
            };

            try
            {
                await postRepo.UpdateAsync(fakePost);
                Console.WriteLine("   Update succeeded (unexpected)");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   Update failed as expected: {ex.GetType().Name}");
            }

            Console.WriteLine("\n5. Handling null/empty string searches:");
            IEnumerable<Author> allAuthors = await authorRepo.Query()
                .Where(a => a.Username != null && a.Username != "")
                .ExecuteAsync();
            Console.WriteLine($"   Authors with non-empty usernames: {allAuthors.Count()}");

            Console.WriteLine("\n6. Delete operations on non-existent records:");
            bool deleteResult = await postRepo.DeleteByIdAsync(99999);
            Console.WriteLine($"   DeleteById(99999) returned: {deleteResult}");

            Console.WriteLine("\n7. Count with always-false condition:");
            int noneCount = await postRepo.CountAsync(p => p.Id < 0);
            Console.WriteLine($"   Count with impossible condition: {noneCount}");

            Console.WriteLine("\n8. Exists with complex condition:");
            bool complexExists = await postRepo.ExistsAsync(p =>
                p.IsPublished == true &&
                p.ViewCount >= 0 &&
                p.Title != ""
            );
            Console.WriteLine($"   Complex existence check: {complexExists}");

            Console.WriteLine();
        }

        #endregion

#pragma warning restore CS8602 // Dereference of a possibly null reference.
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.
    }
}
