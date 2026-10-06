namespace Test.Automated
{
    using System;
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using Test.Shared;

    /// <summary>
    /// Docker image, ports, credentials and environment for one provider's disposable test container.
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
    }
}
