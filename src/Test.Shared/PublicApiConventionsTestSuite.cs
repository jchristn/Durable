namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.CosmosDb;
    using Durable.InMemory;
    using Durable.LiteDb;
    using Durable.LiteGraph;
    using Durable.MySql;
    using Durable.Postgres;
    using Durable.Sql;
    using Durable.Sqlite;
    using Durable.SqlServer;
    using Durable.Tool;
    using Xunit;

    /// <summary>
    /// Enforces the public API conventions over every Durable assembly with reflection (no database): async methods take
    /// a defaulted CancellationToken as their last parameter, awaitable methods carry the Async suffix, every synchronous
    /// I/O member of the repository / query-builder / transaction interfaces has an async counterpart, no public member
    /// exposes a tuple, and no public method of an async-capable type uses out/ref parameters. Justified exceptions live in
    /// the commented allow-lists below; an allow-list entry that no longer matches anything fails the suite so the lists
    /// stay honest.
    /// </summary>
    public class PublicApiConventionsTestSuite
    {
        #region Private-Members

        private static readonly Assembly[] _Assemblies = new Assembly[]
        {
            typeof(IRepository<>).Assembly,
            typeof(ISqlRepository<>).Assembly,
            typeof(SqliteRepository<>).Assembly,
            typeof(MySqlRepository<>).Assembly,
            typeof(PostgresRepository<>).Assembly,
            typeof(SqlServerRepository<>).Assembly,
            typeof(InMemoryBackend).Assembly,
            typeof(LiteDbBackend).Assembly,
            typeof(LiteGraphBackend).Assembly,
            typeof(ConformanceSuites).Assembly,
            typeof(CosmosDbBackend).Assembly,
            typeof(DurableCli).Assembly
        };

        // Async methods that may lack a CancellationToken, or whose token may be required (no "= default").
        // Keys are "Rule:Type.Member" with an optional trailing '*' wildcard.
        private static readonly Dictionary<string, string> _AsyncAllowList = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            // The IRepositoryBackend storage SPI and its implementations: RepositoryBase always flows its caller's token,
            // so SPI tokens are required by design (documented on IRepositoryBackend). Users call the repository, not these.
            { "TokenWithoutDefault:IRepositoryBackend.*", "Backend SPI: the token is always supplied by RepositoryBase." },
            { "TokenWithoutDefault:InMemoryBackend.*", "IRepositoryBackend implementation (SPI token is required)." },
            { "TokenWithoutDefault:LiteDbBackend.*", "IRepositoryBackend implementation (SPI token is required)." },
            { "TokenWithoutDefault:LiteGraphBackend.*", "IRepositoryBackend implementation (SPI token is required)." },
            { "TokenWithoutDefault:BackendIncludeLoader.*", "Backend SPI helper called by RepositoryBase with its token." },
            { "TokenWithoutDefault:CosmosDbBackend.*", "IRepositoryBackend implementation (SPI token is required)." },

            // Unwraps an IAsyncDurableResult without starting enumeration; callers cancel with WithCancellation(token).
            { "MissingToken:RepositoryResultExtensions.AsAsyncEnumerable", "Pure accessor over an existing stream." },
        };

        // Synchronous members of the I/O interfaces that do no I/O (configuration, builders, metadata) and therefore need
        // no async twin.
        private static readonly Dictionary<string, string> _SyncWithoutAsyncAllowList = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "IRepository`1.AddQueryFilter", "Configures the repository in memory." },
            { "IRepository`1.ClearQueryFilters", "Configures the repository in memory." },
            { "IRepository`1.BeginTransaction", "Has BeginTransactionAsync (checked by name)." },
            { "IQueryBuilder`1.ExecuteAsyncEnumerable", "Already async (streaming)." },
            { "IQueryBuilder`1.ExecuteAsyncEnumerableWithQuery", "Already async (streaming with query text)." },
            { "ISqlQueryBuilder`1.BuildSql", "Renders SQL text; no I/O." },
            { "ISqlQueryBuilder`1.BuildStatement", "Renders SQL and parameters; no I/O." },
            { "ISqlTransaction.CreateSavepoint", "Has CreateSavepointAsync (checked by name)." },
            { "IConnectionFactory.OpenConnection", "Has OpenConnectionAsync (checked by name)." },
            { "ITransactionScope.Complete", "Has CompleteAsync (checked by name)." },
            { "*.Dispose", "Paired with DisposeAsync through IAsyncDisposable where the type holds async resources." },
        };

        // Methods of async-capable types allowed to use out/ref parameters.
        private static readonly Dictionary<string, string> _OutRefAllowList = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "*Resolver`1.TryResolveConflict", "Classic synchronous Try pattern; the async twin returns TryResolveConflictResult<T>." },
        };

        private static readonly string[] _IoInterfaces = new string[]
        {
            "IRepository`1", "ISqlRepository`1", "IQueryBuilder`1", "ISqlQueryBuilder`1", "IGroupedQueryBuilder`2",
            "ITransaction", "ISqlTransaction", "ISavepoint", "ITransactionScope", "IConnectionFactory"
        };

        #endregion

        #region Public-Methods

        /// <summary>
        /// Every public async method takes a CancellationToken as its last parameter with a default value (allow-listed
        /// SPI members excepted), and async iterators mark it with [EnumeratorCancellation].
        /// </summary>
        [Fact]
        public void AsyncMethodsTakeDefaultedTokenLast()
        {
            List<string> violations = new List<string>();
            HashSet<string> usedAllowEntries = new HashSet<string>(StringComparer.Ordinal);
            foreach (MethodInfo method in PublicMethods())
            {
                if (!IsAwaitable(method.ReturnType) && !IsAsyncEnumerable(method.ReturnType)) continue;
                if (method.Name == "DisposeAsync") continue;

                ParameterInfo[] parameters = method.GetParameters();
                int tokenIndex = Array.FindIndex(parameters, p => p.ParameterType == typeof(CancellationToken));
                string key = Key(method);
                if (tokenIndex < 0)
                {
                    Report(violations, usedAllowEntries, _AsyncAllowList, "MissingToken", key, Display(method) + ": no CancellationToken");
                    continue;
                }

                if (tokenIndex != parameters.Length - 1)
                    Report(violations, usedAllowEntries, _AsyncAllowList, "TokenNotLast", key, Display(method) + ": CancellationToken is not the last parameter");
                if (!parameters[tokenIndex].HasDefaultValue)
                    Report(violations, usedAllowEntries, _AsyncAllowList, "TokenWithoutDefault", key, Display(method) + ": CancellationToken has no default");
                if (IsAsyncEnumerable(method.ReturnType)
                    && method.GetCustomAttribute<AsyncIteratorStateMachineAttribute>() != null
                    && parameters[tokenIndex].GetCustomAttribute<EnumeratorCancellationAttribute>() == null)
                    Report(violations, usedAllowEntries, _AsyncAllowList, "IteratorWithoutEnumeratorCancellation", key, Display(method) + ": async iterator token lacks [EnumeratorCancellation]");
            }

            AssertClean(violations, usedAllowEntries, _AsyncAllowList, "async signature");
        }

        /// <summary>
        /// No public method returning Task, ValueTask or their generic forms lacks the Async suffix.
        /// </summary>
        [Fact]
        public void AwaitableMethodsHaveAsyncSuffix()
        {
            List<string> violations = PublicMethods()
                .Where(m => IsAwaitable(m.ReturnType) && !m.Name.EndsWith("Async", StringComparison.Ordinal))
                .Select(m => Display(m) + ": awaitable without the Async suffix")
                .ToList();
            Assert.True(violations.Count == 0, "Awaitable methods without the Async suffix:\n" + string.Join("\n", violations));
        }

        /// <summary>
        /// Every synchronous member of the repository, query-builder, transaction and connection interfaces that does I/O
        /// has an async counterpart with the same name plus Async.
        /// </summary>
        [Fact]
        public void SyncIoMembersHaveAsyncCounterparts()
        {
            List<string> violations = new List<string>();
            HashSet<string> usedAllowEntries = new HashSet<string>(StringComparer.Ordinal);
            foreach (Type type in ExportedTypes().Where(t => t.IsInterface && _IoInterfaces.Contains(t.Name)))
            {
                List<MethodInfo> all = type.GetMethods().Concat(type.GetInterfaces().SelectMany(i => i.GetMethods())).ToList();
                HashSet<string> names = new HashSet<string>(all.Select(m => m.Name), StringComparer.Ordinal);
                foreach (MethodInfo method in type.GetMethods().Where(m => !m.IsSpecialName))
                {
                    if (method.Name.EndsWith("Async", StringComparison.Ordinal)) continue;
                    if (IsAwaitable(method.ReturnType) || IsAsyncEnumerable(method.ReturnType)) continue;
                    if (IsBuilder(method.ReturnType)) continue;
                    if (names.Contains(method.Name + "Async")) continue;
                    string key = type.Name + "." + method.Name;
                    if (MatchAllow(_SyncWithoutAsyncAllowList, key, usedAllowEntries)) continue;
                    violations.Add(type.Name + "." + method.Name + ": synchronous member without " + method.Name + "Async");
                }
            }

            // Entries that only document pairs checked by name are expected to stay unused.
            HashSet<string> informational = new HashSet<string>(_SyncWithoutAsyncAllowList.Where(e => e.Value.StartsWith("Has ", StringComparison.Ordinal) || e.Value.StartsWith("Placeholder", StringComparison.Ordinal) || e.Value.StartsWith("Already", StringComparison.Ordinal) || e.Key.StartsWith("*.", StringComparison.Ordinal)).Select(e => e.Key));
            foreach (string key in informational) usedAllowEntries.Add(key);
            AssertClean(violations, usedAllowEntries, _SyncWithoutAsyncAllowList, "sync/async pair");
        }

        /// <summary>
        /// No public member exposes System.Tuple or System.ValueTuple.
        /// </summary>
        [Fact]
        public void NoTuplesInPublicApi()
        {
            List<string> violations = new List<string>();
            foreach (Type type in ExportedTypes())
            {
                foreach (MemberInfo member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
                {
                    if (member is PropertyInfo property && ContainsTuple(property.PropertyType)) violations.Add(type.Name + "." + member.Name);
                    if (member is FieldInfo field && ContainsTuple(field.FieldType)) violations.Add(type.Name + "." + member.Name);
                    if (member is MethodBase method)
                    {
                        if (method is MethodInfo info && ContainsTuple(info.ReturnType)) violations.Add(type.Name + "." + member.Name + " (return)");
                        foreach (ParameterInfo parameter in method.GetParameters())
                            if (ContainsTuple(parameter.ParameterType)) violations.Add(type.Name + "." + member.Name + " (" + parameter.Name + ")");
                    }
                }
            }

            Assert.True(violations.Count == 0, "Tuples in the public API:\n" + string.Join("\n", violations.Distinct()));
        }

        /// <summary>
        /// No public method of a type that has async methods takes out or ref parameters (they cannot have async twins).
        /// </summary>
        [Fact]
        public void NoOutOrRefOnAsyncCapableTypes()
        {
            List<string> violations = new List<string>();
            HashSet<string> usedAllowEntries = new HashSet<string>(StringComparer.Ordinal);
            foreach (Type type in ExportedTypes())
            {
                MethodInfo[] methods = type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly);
                bool asyncCapable = methods.Any(m => m.Name != "DisposeAsync" && (IsAwaitable(m.ReturnType) || IsAsyncEnumerable(m.ReturnType)));
                if (!asyncCapable) continue;
                foreach (MethodInfo method in methods.Where(m => m.GetParameters().Any(p => p.ParameterType.IsByRef)))
                {
                    if (method.GetParameters().All(p => !p.ParameterType.IsByRef || p.IsIn)) continue;
                    string key = type.Name + "." + method.Name;
                    if (MatchAllow(_OutRefAllowList, key, usedAllowEntries)) continue;
                    violations.Add(Display(method) + ": out/ref parameter on an async-capable type");
                }
            }

            AssertClean(violations, usedAllowEntries, _OutRefAllowList, "out/ref");
        }

        #endregion

        #region Private-Methods

        private static IEnumerable<Type> ExportedTypes()
        {
            return _Assemblies.SelectMany(a => a.GetExportedTypes()).Where(t => !typeof(Delegate).IsAssignableFrom(t));
        }

        private static IEnumerable<MethodInfo> PublicMethods()
        {
            foreach (Type type in ExportedTypes())
            {
                foreach (MethodInfo method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
                {
                    if (method.IsSpecialName) continue;
                    yield return method;
                }
            }
        }

        private static bool IsAwaitable(Type type)
        {
            if (type == typeof(Task) || type == typeof(ValueTask)) return true;
            if (!type.IsGenericType) return false;
            Type definition = type.GetGenericTypeDefinition();
            return definition == typeof(Task<>) || definition == typeof(ValueTask<>);
        }

        private static bool IsAsyncEnumerable(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>);
        }

        private static bool IsBuilder(Type type)
        {
            return type.IsInterface && (type.Name.Contains("QueryBuilder", StringComparison.Ordinal) || type.Name.Contains("ExpressionBuilder", StringComparison.Ordinal));
        }

        private static bool ContainsTuple(Type type)
        {
            if (type.IsByRef || type.IsArray) return ContainsTuple(type.GetElementType()!);
            if (type.FullName != null && (type.FullName.StartsWith("System.ValueTuple", StringComparison.Ordinal) || type.FullName.StartsWith("System.Tuple", StringComparison.Ordinal))) return true;
            return type.IsGenericType && type.GetGenericArguments().Any(ContainsTuple);
        }

        private static string Key(MethodInfo method)
        {
            return StripArity(method.DeclaringType!.Name) + "." + method.Name;
        }

        private static string StripArity(string name)
        {
            int tick = name.IndexOf('`');
            return tick < 0 ? name : name.Substring(0, tick);
        }

        private static string Display(MethodInfo method)
        {
            return method.DeclaringType!.Assembly.GetName().Name + ": " + method.DeclaringType.Name + "." + method.Name
                + "(" + string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name + " " + p.Name)) + ")";
        }

        private static void Report(List<string> violations, HashSet<string> used, Dictionary<string, string> allowList, string rule, string key, string message)
        {
            if (MatchAllow(allowList, rule + ":" + key, used)) return;
            violations.Add(rule + " " + message);
        }

        private static bool MatchAllow(Dictionary<string, string> allowList, string key, HashSet<string> used)
        {
            foreach (string pattern in allowList.Keys)
            {
                if (Matches(pattern, key))
                {
                    used.Add(pattern);
                    return true;
                }
            }

            return false;
        }

        private static bool Matches(string pattern, string key)
        {
            string[] parts = pattern.Split('*');
            int position = 0;
            for (int i = 0; i < parts.Length; i++)
            {
                string part = parts[i];
                if (part.Length == 0) continue;
                int found = key.IndexOf(part, position, StringComparison.Ordinal);
                if (found < 0) return false;
                if (i == 0 && found != 0) return false;
                position = found + part.Length;
            }

            return pattern.EndsWith("*", StringComparison.Ordinal) || position == key.Length;
        }

        private static void AssertClean(List<string> violations, HashSet<string> used, Dictionary<string, string> allowList, string rule)
        {
            List<string> stale = allowList.Keys.Where(k => !used.Contains(k)).ToList();
            Assert.True(violations.Count == 0, "Public API " + rule + " violations:\n" + string.Join("\n", violations));
            Assert.True(stale.Count == 0, "Stale " + rule + " allow-list entries (remove them):\n" + string.Join("\n", stale));
        }

        #endregion
    }
}
