namespace Test.Automated
{
    using System;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Test.Shared;

    /// <summary>
    /// Docker image, ports, credentials and environment for one provider's disposable test container.
    /// Each target fills in its own Create* method; <see cref="DockerizedDatabaseSession"/> needs nothing else unless
    /// the target wants a readiness check other than the default (see <see cref="ReadinessProbe"/>).
    /// </summary>
    internal sealed class ProviderDockerSettings
    {
        private static readonly Regex StrongSqlServerPasswordPattern =
            new Regex(@"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,128}$", RegexOptions.Compiled);

        private ProviderDockerSettings()
        {
        }

        public string ProviderSlug { get; private init; } = string.Empty;

        public string ImageName { get; private init; } = string.Empty;

        public int ContainerPort { get; private init; }

        public int? HostPort { get; private init; }

        public IReadOnlyDictionary<string, string> EnvironmentVariables { get; private init; } = new Dictionary<string, string>();

        public TestDatabaseType DatabaseType { get; private init; }

        public string DatabaseName { get; private init; } = string.Empty;

        public string Username { get; private init; } = string.Empty;

        public string Password { get; private init; } = string.Empty;

        public string? SqlServerAdminPassword { get; private init; }

        public bool Debug { get; private init; }

        public string? Schema { get; private init; }

        /// <summary>
        /// Gets extra "docker run" arguments placed before the image (for example "--memory", "1g"). Default: empty. Never null.
        /// </summary>
        public IReadOnlyList<string> ExtraRunArguments { get; private init; } = Array.Empty<string>();

        /// <summary>
        /// Gets the arguments placed after the image and passed to the container's entry point (for example
        /// "start-single-node", "--insecure"). Default: empty. Never null.
        /// </summary>
        public IReadOnlyList<string> ContainerCommand { get; private init; } = Array.Empty<string>();

        /// <summary>
        /// Gets how long <see cref="DockerizedDatabaseSession"/> waits for readiness. Default: 3 minutes.
        /// </summary>
        public TimeSpan StartupTimeout { get; private init; } = TimeSpan.FromMinutes(3);

        /// <summary>
        /// Gets the readiness probe: it throws until the container is ready and is retried until <see cref="StartupTimeout"/>
        /// (a <see cref="NotSupportedException"/> stops the retries). It may also prepare the database idempotently. Null
        /// (default) uses the built-in waits for MySQL, PostgreSQL and SQL Server, <see cref="DocumentBackendTestTargets.ProbeAsync"/>
        /// for document backends, and <see cref="RepositoryProviderFactory.Create"/> plus
        /// <see cref="IRepositoryProvider.IsDatabaseAvailableAsync"/> for every other SQL target.
        /// </summary>
        public Func<TestRuntimeConfiguration, CancellationToken, Task>? ReadinessProbe { get; private init; }

        public static ProviderDockerSettings Create(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            switch (configuration.DatabaseType)
            {
                case TestDatabaseType.MySql:
                    return CreateMysql(configuration, dockerImageOverride);
                case TestDatabaseType.Postgres:
                    return CreatePostgresql(configuration, dockerImageOverride);
                case TestDatabaseType.SqlServer:
                    return CreateSqlServer(configuration, dockerImageOverride);

                // v0.7.0 targets: each one fills in its own Create* method below.
                case TestDatabaseType.Oracle:
                    return CreateOracle(configuration, dockerImageOverride);

                case TestDatabaseType.MariaDb:
                    return CreateMariaDb(configuration, dockerImageOverride);

                case TestDatabaseType.CockroachDb:
                    return CreateCockroachDb(configuration, dockerImageOverride);

                case TestDatabaseType.YugabyteDb:
                    return CreateYugabyteDb(configuration, dockerImageOverride);

                case TestDatabaseType.MongoDb:
                    return CreateMongoDb(configuration, dockerImageOverride);

                case TestDatabaseType.CosmosDb:
                    return CreateCosmosDb(configuration, dockerImageOverride);

                default:
                    throw new InvalidOperationException("Unsupported dockerized database type " + configuration.DatabaseType + ".");
            }
        }

        public TestRuntimeConfiguration BuildEffectiveConfiguration(int hostPort)
        {
            return new TestRuntimeConfiguration
            {
                DatabaseType = DatabaseType,
                DatabaseName = DatabaseName,
                Hostname = "127.0.0.1",
                Port = hostPort,
                Username = Username,
                Password = Password,
                Debug = Debug,
                Schema = Schema
            };
        }

        private static ProviderDockerSettings CreateMysql(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            string username = string.IsNullOrWhiteSpace(configuration.Username) ? "root" : configuration.Username;
            string password = string.IsNullOrWhiteSpace(configuration.Password) ? "password" : configuration.Password;
            string databaseName = string.IsNullOrWhiteSpace(configuration.DatabaseName) ? "durable_touchstone" : configuration.DatabaseName;

            Dictionary<string, string> environmentVariables = new Dictionary<string, string>
            {
                ["MYSQL_ROOT_PASSWORD"] = string.IsNullOrWhiteSpace(configuration.Password) ? "password" : configuration.Password,
                ["MYSQL_DATABASE"] = databaseName
            };

            if (!username.Equals("root", StringComparison.OrdinalIgnoreCase))
            {
                environmentVariables["MYSQL_USER"] = username;
                environmentVariables["MYSQL_PASSWORD"] = password;
                environmentVariables["MYSQL_ROOT_PASSWORD"] = "root-password";
            }

            return new ProviderDockerSettings
            {
                ProviderSlug = "mysql",
                ImageName = string.IsNullOrWhiteSpace(dockerImageOverride) ? "mysql:8.4" : dockerImageOverride,
                ContainerPort = 3306,
                HostPort = configuration.Port,
                EnvironmentVariables = environmentVariables,
                DatabaseType = TestDatabaseType.MySql,
                DatabaseName = databaseName,
                Username = username,
                Password = password,
                Debug = configuration.Debug,
                Schema = configuration.Schema
            };
        }

        private static ProviderDockerSettings CreatePostgresql(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            string username = string.IsNullOrWhiteSpace(configuration.Username) ? "postgres" : configuration.Username;
            string password = string.IsNullOrWhiteSpace(configuration.Password) ? "password" : configuration.Password;
            string databaseName = string.IsNullOrWhiteSpace(configuration.DatabaseName) ? "durable_touchstone" : configuration.DatabaseName;

            return new ProviderDockerSettings
            {
                ProviderSlug = "postgres",
                ImageName = string.IsNullOrWhiteSpace(dockerImageOverride) ? "postgres:16" : dockerImageOverride,
                ContainerPort = 5432,
                HostPort = configuration.Port,
                EnvironmentVariables = new Dictionary<string, string>
                {
                    ["POSTGRES_DB"] = databaseName,
                    ["POSTGRES_USER"] = username,
                    ["POSTGRES_PASSWORD"] = password
                },
                DatabaseType = TestDatabaseType.Postgres,
                DatabaseName = databaseName,
                Username = username,
                Password = password,
                Debug = configuration.Debug,
                Schema = configuration.Schema
            };
        }

        private static ProviderDockerSettings CreateSqlServer(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            string username = string.IsNullOrWhiteSpace(configuration.Username) ? "sa" : configuration.Username;
            string databaseName = string.IsNullOrWhiteSpace(configuration.DatabaseName) ? "durable_touchstone" : configuration.DatabaseName;
            string saPassword;
            string effectivePassword;

            if (username.Equals("sa", StringComparison.OrdinalIgnoreCase))
            {
                effectivePassword = string.IsNullOrWhiteSpace(configuration.Password) ? "Durable!Pass123" : configuration.Password;
                if (!StrongSqlServerPasswordPattern.IsMatch(effectivePassword))
                {
                    throw new InvalidOperationException(
                        "When using --type sqlserver --docker with the sa login, --pass must contain upper, lower, number, and symbol characters and be at least 8 characters long.");
                }

                saPassword = effectivePassword;
            }
            else
            {
                effectivePassword = string.IsNullOrWhiteSpace(configuration.Password) ? "password" : configuration.Password;
                saPassword = "Durable!SaPass123";
            }

            return new ProviderDockerSettings
            {
                ProviderSlug = "sqlserver",
                ImageName = string.IsNullOrWhiteSpace(dockerImageOverride) ? "mcr.microsoft.com/mssql/server:2022-latest" : dockerImageOverride,
                ContainerPort = 1433,
                HostPort = configuration.Port,
                EnvironmentVariables = new Dictionary<string, string>
                {
                    ["ACCEPT_EULA"] = "Y",
                    ["MSSQL_PID"] = "Developer",
                    ["MSSQL_SA_PASSWORD"] = saPassword
                },
                DatabaseType = TestDatabaseType.SqlServer,
                DatabaseName = databaseName,
                Username = username,
                Password = effectivePassword,
                SqlServerAdminPassword = saPassword,
                Debug = configuration.Debug,
                Schema = configuration.Schema
            };
        }

        private static ProviderDockerSettings CreateOracle(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.Oracle, "docker settings");
        }

        private static ProviderDockerSettings CreateMariaDb(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.MariaDb, "docker settings");
        }

        private static ProviderDockerSettings CreateCockroachDb(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.CockroachDb, "docker settings");
        }

        private static ProviderDockerSettings CreateYugabyteDb(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            throw TestDatabaseTypes.NotYetAvailable(TestDatabaseType.YugabyteDb, "docker settings");
        }

        private static ProviderDockerSettings CreateMongoDb(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            // A single-node replica set (multi-document transactions need one), without authentication. The readiness
            // probe initiates the replica set, waits for a writable primary, then runs the default document-backend probe.
            string databaseName = string.IsNullOrWhiteSpace(configuration.DatabaseName) ? "durable_touchstone" : configuration.DatabaseName;
            return new ProviderDockerSettings
            {
                ProviderSlug = "mongodb",
                ImageName = string.IsNullOrWhiteSpace(dockerImageOverride) ? "mongo:8" : dockerImageOverride,
                ContainerPort = 27017,
                HostPort = configuration.Port,
                DatabaseType = TestDatabaseType.MongoDb,
                DatabaseName = databaseName,
                Username = string.Empty,
                Password = string.Empty,
                Debug = configuration.Debug,
                Schema = configuration.Schema,
                ExtraRunArguments = new[] { "--memory", "1g" },
                ContainerCommand = new[] { "--replSet", "rs0", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25" },
                StartupTimeout = TimeSpan.FromMinutes(3),
                ReadinessProbe = async (effective, token) =>
                {
                    await MongoDbTestTarget.InitiateReplicaSetAsync(effective, token).ConfigureAwait(false);
                    await DocumentBackendTestTargets.ProbeAsync(effective, token).ConfigureAwait(false);
                }
            };
        }

        private static ProviderDockerSettings CreateCosmosDb(TestRuntimeConfiguration configuration, string? dockerImageOverride)
        {
            // The Linux "vNext" emulator (arm64 and x64) speaks plain HTTP on 8081 with --protocol http, supports only gateway
            // mode, and accepts the well-known emulator key. The test target disables endpoint discovery so the random host
            // port works. Readiness is DocumentBackendTestTargets.ProbeAsync (connect and create the database).
            string databaseName = string.IsNullOrWhiteSpace(configuration.DatabaseName) ? "durable_touchstone" : configuration.DatabaseName;
            string password = string.IsNullOrWhiteSpace(configuration.Password) ? "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==" : configuration.Password;

            return new ProviderDockerSettings
            {
                ProviderSlug = "cosmosdb",
                ImageName = string.IsNullOrWhiteSpace(dockerImageOverride) ? "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview" : dockerImageOverride,
                ContainerPort = 8081,
                HostPort = configuration.Port,
                ExtraRunArguments = new[] { "--memory", "2g" },
                ContainerCommand = new[] { "--protocol", "http", "--enable-explorer", "false" },
                StartupTimeout = TimeSpan.FromMinutes(4),
                DatabaseType = TestDatabaseType.CosmosDb,
                DatabaseName = databaseName,
                Username = string.Empty,
                Password = password,
                Debug = configuration.Debug,
                Schema = configuration.Schema
            };
        }
    }
}
