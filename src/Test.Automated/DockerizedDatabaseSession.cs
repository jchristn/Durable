namespace Test.Automated
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using MySqlConnector;
    using Npgsql;
    using Test.Shared;

    /// <summary>
    /// Starts a disposable dockerized database (any server-based provider or document backend with a
    /// <see cref="ProviderDockerSettings"/> case) on an ephemeral host port, waits for readiness, and removes the container
    /// on disposal. In-process providers (SQLite, DuckDB) do not use docker.
    /// </summary>
    internal sealed class DockerizedDatabaseSession : IAsyncDisposable
    {
        #region Private-Members

        private static readonly TimeSpan PollDelay = TimeSpan.FromSeconds(2);
        private static readonly object LifecycleSyncRoot = new object();
        private static readonly Dictionary<string, bool> ActiveContainers = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        private static bool _LifecycleHandlersRegistered;

        private readonly bool _KeepContainer;
        private int _Disposed;

        #endregion

        #region Public-Members

        public string ContainerName { get; }

        public string ImageName { get; }

        public TestRuntimeConfiguration Configuration { get; }

        #endregion

        #region Constructors-and-Factories

        private DockerizedDatabaseSession(string containerName, string imageName, TestRuntimeConfiguration configuration, bool keepContainer)
        {
            ContainerName = containerName;
            ImageName = imageName;
            Configuration = configuration;
            _KeepContainer = keepContainer;
        }

        public static async Task<DockerizedDatabaseSession> StartAsync(
            TestRuntimeConfiguration requestedConfiguration,
            string? dockerImageOverride,
            bool keepContainer)
        {
            if (requestedConfiguration == null) throw new ArgumentNullException(nameof(requestedConfiguration));
            if (TestDatabaseTypes.IsInProcess(requestedConfiguration.DatabaseType))
            {
                throw new InvalidOperationException(
                    "The --docker option is not used for " + TestDatabaseTypes.ProviderTag(requestedConfiguration.DatabaseType) + ", which runs in-process.");
            }

            if (!string.IsNullOrWhiteSpace(requestedConfiguration.Filename))
            {
                throw new InvalidOperationException("Do not supply --filename with --docker.");
            }

            if (!string.IsNullOrWhiteSpace(requestedConfiguration.Hostname)
                && !requestedConfiguration.Hostname.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
                && !requestedConfiguration.Hostname.Equals("localhost", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("The --host option must be localhost or 127.0.0.1 when using --docker.");
            }

            await EnsureDockerIsAvailableAsync();

            ProviderDockerSettings settings = ProviderDockerSettings.Create(requestedConfiguration, dockerImageOverride);
            string containerName = "durable-touchstone-" + settings.ProviderSlug + "-" + Guid.NewGuid().ToString("N").Substring(0, 12);

            try
            {
                await RunDockerCommandAsync(BuildRunArguments(containerName, settings).ToArray());
                RegisterActiveContainer(containerName, keepContainer);

                int hostPort = settings.HostPort ?? await ResolvePublishedPortAsync(containerName, settings.ContainerPort);

                TestRuntimeConfiguration effectiveConfiguration = settings.BuildEffectiveConfiguration(hostPort);
                DockerizedDatabaseSession session = new DockerizedDatabaseSession(
                    containerName,
                    settings.ImageName,
                    effectiveConfiguration,
                    keepContainer);

                await session.InitializeProviderAsync(settings);

                Console.WriteLine(
                    "Started dockerized " + settings.ProviderSlug
                    + " test database in container '" + containerName
                    + "' using image '" + settings.ImageName
                    + "' on 127.0.0.1:" + hostPort.ToString(CultureInfo.InvariantCulture) + ".");

                if (keepContainer)
                {
                    Console.WriteLine("Container will be left running because --keep-docker was supplied.");
                }

                return session;
            }
            catch
            {
                await ReleaseContainerAsync(containerName, keepContainer: false);
                throw;
            }
        }

        #endregion

        #region Public-Methods

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _Disposed, 1) != 0)
            {
                return;
            }

            if (_KeepContainer)
            {
                UnregisterActiveContainer(ContainerName);
                Console.WriteLine("Leaving docker container '" + ContainerName + "' running.");
                return;
            }

            await ReleaseContainerAsync(ContainerName, keepContainer: false);
        }

        #endregion

        #region Private-Methods

        private async Task InitializeProviderAsync(ProviderDockerSettings settings)
        {
            TimeSpan timeout = settings.StartupTimeout;
            string description = TestDatabaseTypes.ProviderName(Configuration.DatabaseType) + " container readiness";

            if (settings.ReadinessProbe != null)
            {
                Func<TestRuntimeConfiguration, CancellationToken, Task> probe = settings.ReadinessProbe;
                await WaitUntilAsync(description, () => probe(Configuration, CancellationToken.None), timeout);
                return;
            }

            if (TestDatabaseTypes.IsDocumentBackend(Configuration.DatabaseType))
            {
                await WaitUntilAsync(description, () => DocumentBackendTestTargets.ProbeAsync(Configuration, CancellationToken.None), timeout);
                return;
            }

            switch (Configuration.DatabaseType)
            {
                case TestDatabaseType.MySql:
                    await WaitForMysqlAsync(Configuration);
                    break;
                case TestDatabaseType.Postgres:
                    await WaitForPostgresqlAsync(Configuration);
                    break;
                case TestDatabaseType.SqlServer:
                    await WaitForSqlServerServerAsync(settings.SqlServerAdminPassword!);
                    await EnsureSqlServerDatabaseAndLoginAsync(settings);
                    await WaitForSqlServerDatabaseAsync(Configuration);
                    break;
                default:
                    await WaitUntilAsync(description, () => ProbeSqlProviderAsync(Configuration), timeout);
                    break;
            }
        }

        private static async Task ProbeSqlProviderAsync(TestRuntimeConfiguration configuration)
        {
            using IRepositoryProvider provider = RepositoryProviderFactory.Create(configuration);
            if (!await provider.IsDatabaseAvailableAsync())
            {
                throw new InvalidOperationException(provider.ProviderName + " is not accepting connections yet.");
            }
        }

        private async Task WaitForSqlServerServerAsync(string saPassword)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = Configuration.Hostname + "," + Configuration.Port,
                UserID = "sa",
                Password = saPassword,
                InitialCatalog = "master",
                Encrypt = false,
                TrustServerCertificate = true
            };

            await WaitUntilAsync(
                "SQL Server container readiness",
                async () =>
                {
                    using SqlConnection connection = new SqlConnection(builder.ConnectionString);
                    await connection.OpenAsync();
                    using SqlCommand command = new SqlCommand("SELECT 1;", connection);
                    await command.ExecuteScalarAsync();
                });
        }

        private async Task EnsureSqlServerDatabaseAndLoginAsync(ProviderDockerSettings settings)
        {
            SqlConnectionStringBuilder masterBuilder = new SqlConnectionStringBuilder
            {
                DataSource = Configuration.Hostname + "," + Configuration.Port,
                UserID = "sa",
                Password = settings.SqlServerAdminPassword,
                InitialCatalog = "master",
                Encrypt = false,
                TrustServerCertificate = true
            };

            using SqlConnection masterConnection = new SqlConnection(masterBuilder.ConnectionString);
            await masterConnection.OpenAsync();

            string databaseName = QuoteSqlServerIdentifier(Configuration.DatabaseName);
            string databaseLiteral = EscapeSqlLiteral(Configuration.DatabaseName);

            string createDatabase =
                "IF DB_ID(N'" + databaseLiteral + "') IS NULL " +
                "BEGIN EXEC('CREATE DATABASE " + databaseName + "'); END;";

            await ExecuteNonQueryAsync(masterConnection, createDatabase);

            string username = Configuration.Username ?? throw new InvalidOperationException("SQL Server docker configuration did not provide a username.");

            if (!username.Equals("sa", StringComparison.OrdinalIgnoreCase))
            {
                string loginName = QuoteSqlServerIdentifier(username);
                string loginLiteral = EscapeSqlLiteral(username);
                string passwordLiteral = EscapeSqlLiteral(Configuration.Password!);

                string createLogin =
                    "IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = N'" + loginLiteral + "') " +
                    "BEGIN EXEC('CREATE LOGIN " + loginName + " WITH PASSWORD = ''''" + passwordLiteral + "'''', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF'); END;";

                await ExecuteNonQueryAsync(masterConnection, createLogin);

                SqlConnectionStringBuilder databaseBuilder = new SqlConnectionStringBuilder(masterBuilder.ConnectionString)
                {
                    InitialCatalog = Configuration.DatabaseName
                };

                using SqlConnection databaseConnection = new SqlConnection(databaseBuilder.ConnectionString);
                await databaseConnection.OpenAsync();

                string createUser =
                    "IF DATABASE_PRINCIPAL_ID(N'" + loginLiteral + "') IS NULL " +
                    "BEGIN EXEC('CREATE USER " + loginName + " FOR LOGIN " + loginName + "'); END;" +
                    "IF NOT EXISTS (" +
                    "SELECT 1 FROM sys.database_role_members drm " +
                    "INNER JOIN sys.database_principals rolep ON drm.role_principal_id = rolep.principal_id " +
                    "INNER JOIN sys.database_principals memberp ON drm.member_principal_id = memberp.principal_id " +
                    "WHERE rolep.name = N'db_owner' AND memberp.name = N'" + loginLiteral + "')" +
                    "BEGIN ALTER ROLE [db_owner] ADD MEMBER " + loginName + "; END;";

                await ExecuteNonQueryAsync(databaseConnection, createUser);
            }
        }

        private async Task WaitForSqlServerDatabaseAsync(TestRuntimeConfiguration configuration)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
            {
                DataSource = configuration.Hostname + "," + configuration.Port,
                UserID = configuration.Username,
                Password = configuration.Password,
                InitialCatalog = configuration.DatabaseName,
                Encrypt = false,
                TrustServerCertificate = true
            };

            await WaitUntilAsync(
                "SQL Server database readiness",
                async () =>
                {
                    using SqlConnection connection = new SqlConnection(builder.ConnectionString);
                    await connection.OpenAsync();
                    using SqlCommand command = new SqlCommand("SELECT 1;", connection);
                    await command.ExecuteScalarAsync();
                });
        }

        private static async Task WaitForMysqlAsync(TestRuntimeConfiguration configuration)
        {
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder
            {
                Server = configuration.Hostname,
                Port = (uint)(configuration.Port ?? 3306),
                UserID = configuration.Username,
                Password = configuration.Password,
                Database = configuration.DatabaseName,
                AllowUserVariables = true,
                Pooling = true
            };

            await WaitUntilAsync(
                "MySQL container readiness",
                async () =>
                {
                    using MySqlConnection connection = new MySqlConnection(builder.ConnectionString);
                    await connection.OpenAsync();
                    using MySqlCommand command = new MySqlCommand("SELECT 1;", connection);
                    await command.ExecuteScalarAsync();
                });
        }

        private static async Task WaitForPostgresqlAsync(TestRuntimeConfiguration configuration)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder
            {
                Host = configuration.Hostname,
                Port = configuration.Port ?? 5432,
                Username = configuration.Username,
                Password = configuration.Password,
                Database = configuration.DatabaseName,
                Pooling = true
            };

            await WaitUntilAsync(
                "PostgreSQL container readiness",
                async () =>
                {
                    using NpgsqlConnection connection = new NpgsqlConnection(builder.ConnectionString);
                    await connection.OpenAsync();
                    using NpgsqlCommand command = new NpgsqlCommand("SELECT 1;", connection);
                    await command.ExecuteScalarAsync();
                });
        }

        private static async Task WaitUntilAsync(string description, Func<Task> action, TimeSpan? timeout = null)
        {
            DateTime deadline = DateTime.UtcNow.Add(timeout ?? TimeSpan.FromMinutes(3));
            Exception? lastException = null;

            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    await action();
                    return;
                }
                catch (NotSupportedException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    lastException = e;
                    await Task.Delay(PollDelay);
                }
            }

            throw new TimeoutException("Timed out waiting for " + description + ".", lastException);
        }

        private static async Task<int> ResolvePublishedPortAsync(string containerName, int containerPort)
        {
            DockerCommandResult result = await RunDockerCommandAsync(
                "inspect",
                "--format",
                "{{(index (index .NetworkSettings.Ports \"" + containerPort.ToString(CultureInfo.InvariantCulture) + "/tcp\") 0).HostPort}}",
                containerName);

            if (!int.TryParse(result.StandardOutput.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int port))
            {
                throw new InvalidOperationException(
                    "Unable to determine published port for container '" + containerName + "'. Docker reported: " + result.StandardOutput);
            }

            return port;
        }

        private static IReadOnlyList<string> BuildRunArguments(string containerName, ProviderDockerSettings settings)
        {
            List<string> arguments = new List<string>
            {
                "run",
                "--detach",
                "--name",
                containerName,
                "--label",
                "durable.touchstone=true"
            };

            string publishedPort = settings.HostPort.HasValue
                ? "127.0.0.1:" + settings.HostPort.Value.ToString(CultureInfo.InvariantCulture) + ":" + settings.ContainerPort.ToString(CultureInfo.InvariantCulture)
                : "127.0.0.1::" + settings.ContainerPort.ToString(CultureInfo.InvariantCulture);

            arguments.Add("--publish");
            arguments.Add(publishedPort);

            foreach (KeyValuePair<string, string> environmentVariable in settings.EnvironmentVariables)
            {
                arguments.Add("--env");
                arguments.Add(environmentVariable.Key + "=" + environmentVariable.Value);
            }

            arguments.AddRange(settings.ExtraRunArguments);
            arguments.Add(settings.ImageName);
            arguments.AddRange(settings.ContainerCommand);
            return arguments;
        }

        private static async Task EnsureDockerIsAvailableAsync()
        {
            await RunDockerCommandAsync("info", "--format", "{{.ServerVersion}}");
        }

        private static void RegisterActiveContainer(string containerName, bool keepContainer)
        {
            if (string.IsNullOrWhiteSpace(containerName))
            {
                return;
            }

            lock (LifecycleSyncRoot)
            {
                if (!_LifecycleHandlersRegistered)
                {
                    AppDomain.CurrentDomain.ProcessExit += (_, _) => CleanupTrackedContainersOnShutdown("process exit");
                    AppDomain.CurrentDomain.UnhandledException += (_, _) => CleanupTrackedContainersOnShutdown("unhandled exception");
                    Console.CancelKeyPress += HandleConsoleCancelKeyPress;
                    _LifecycleHandlersRegistered = true;
                }

                ActiveContainers[containerName] = keepContainer;
            }
        }

        private static void UnregisterActiveContainer(string containerName)
        {
            if (string.IsNullOrWhiteSpace(containerName))
            {
                return;
            }

            lock (LifecycleSyncRoot)
            {
                ActiveContainers.Remove(containerName);
            }
        }

        private static async Task ReleaseContainerAsync(string containerName, bool keepContainer)
        {
            if (string.IsNullOrWhiteSpace(containerName))
            {
                return;
            }

            bool leaveRunning = keepContainer;

            lock (LifecycleSyncRoot)
            {
                if (ActiveContainers.TryGetValue(containerName, out bool trackedKeepContainer))
                {
                    leaveRunning = trackedKeepContainer;
                    ActiveContainers.Remove(containerName);
                }
            }

            if (leaveRunning)
            {
                Console.WriteLine("Leaving docker container '" + containerName + "' running.");
                return;
            }

            await TryStopContainerAsync(containerName);
            bool removed = await TryRemoveContainerAsync(containerName);
            if (!removed)
            {
                throw new InvalidOperationException("Failed to remove docker container '" + containerName + "'.");
            }

            Console.WriteLine("Stopped and removed docker container '" + containerName + "'.");
        }

        private static async Task ExecuteNonQueryAsync(SqlConnection connection, string sql)
        {
            using SqlCommand command = new SqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }

        private static async Task<DockerCommandResult> RunDockerCommandAsync(params string[] arguments)
        {
            System.Diagnostics.ProcessStartInfo startInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = "docker",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            foreach (string argument in arguments)
            {
                startInfo.ArgumentList.Add(argument);
            }

            using System.Diagnostics.Process process = new System.Diagnostics.Process { StartInfo = startInfo };
            process.Start();

            Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync();
            Task<string> stderrTask = process.StandardError.ReadToEndAsync();

            await process.WaitForExitAsync();

            string stdout = await stdoutTask;
            string stderr = await stderrTask;

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    "Docker command failed: docker " + string.Join(" ", arguments) + Environment.NewLine +
                    "Exit code: " + process.ExitCode.ToString(CultureInfo.InvariantCulture) + Environment.NewLine +
                    "Stdout: " + stdout + Environment.NewLine +
                    "Stderr: " + stderr);
            }

            return new DockerCommandResult(stdout, stderr);
        }

        private static async Task<bool> TryStopContainerAsync(string containerName)
        {
            if (string.IsNullOrWhiteSpace(containerName))
            {
                return true;
            }

            try
            {
                await RunDockerCommandAsync("stop", "--time", "15", containerName);
                return true;
            }
            catch (Exception e)
            {
                if (IsMissingContainerException(e))
                {
                    return true;
                }

                Console.Error.WriteLine("Failed to stop docker container '" + containerName + "': " + e.Message);
                return false;
            }
        }

        private static async Task<bool> TryRemoveContainerAsync(string containerName)
        {
            if (string.IsNullOrWhiteSpace(containerName))
            {
                return true;
            }

            try
            {
                await RunDockerCommandAsync("rm", "--force", "--volumes", containerName);
                return true;
            }
            catch (Exception e)
            {
                if (IsMissingContainerException(e))
                {
                    return true;
                }

                Console.Error.WriteLine("Failed to remove docker container '" + containerName + "': " + e.Message);
                return false;
            }
        }

        private static void HandleConsoleCancelKeyPress(object? sender, ConsoleCancelEventArgs args)
        {
            CleanupTrackedContainersOnShutdown("console cancel");
        }

        private static void CleanupTrackedContainersOnShutdown(string reason)
        {
            KeyValuePair<string, bool>[] containers;

            lock (LifecycleSyncRoot)
            {
                containers = ActiveContainers.ToArray();
                ActiveContainers.Clear();
            }

            foreach (KeyValuePair<string, bool> container in containers)
            {
                if (container.Value)
                {
                    Console.WriteLine(
                        "Leaving docker container '" + container.Key + "' running during " + reason + " because --keep-docker was supplied.");
                    continue;
                }

                try
                {
                    _ = TryStopContainerAsync(container.Key).GetAwaiter().GetResult();
                    bool removed = TryRemoveContainerAsync(container.Key).GetAwaiter().GetResult();
                    if (!removed)
                    {
                        Console.Error.WriteLine(
                            "Failed to remove docker container '" + container.Key + "' during " + reason + ".");
                    }
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine(
                        "Failed to stop and remove docker container '" + container.Key + "' during " + reason + ": " + e.Message);
                }
            }
        }

        private static bool IsMissingContainerException(Exception exception)
        {
            return exception.Message.Contains("No such container", StringComparison.OrdinalIgnoreCase)
                || exception.Message.Contains("is not running", StringComparison.OrdinalIgnoreCase);
        }

        private static string EscapeSqlLiteral(string value)
        {
            return value.Replace("'", "''", StringComparison.Ordinal);
        }

        private static string QuoteSqlServerIdentifier(string value)
        {
            return "[" + value.Replace("]", "]]", StringComparison.Ordinal) + "]";
        }

        #endregion
    }
}
