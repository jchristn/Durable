namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Durable;
    using Durable.Conformance;
    using Durable.Sql;

    /// <summary>
    /// Runs the Durable.Conformance kit against a SQL provider: repositories come from an <see cref="IRepositoryProvider"/>,
    /// and resetting storage drops each table and recreates it with <see cref="ISqlRepository{T}.InitializeTable"/>.
    /// SQL repositories support <see cref="RepositoryCapabilities.All"/>.
    /// Thread safety: stateless apart from the provider accessor; the kit calls it sequentially.
    /// </summary>
    public sealed class SqlConformanceTarget : IConformanceTarget
    {
        #region Public-Members

        /// <summary>
        /// Gets the display name (for example "SQLite"). Never null.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets <see cref="RepositoryCapabilities.All"/>.
        /// </summary>
        public RepositoryCapabilities Capabilities => RepositoryCapabilities.All;

        #endregion

        #region Private-Members

        private static readonly MethodInfo _ResetTypeMethod = typeof(SqlConformanceTarget).GetMethod(nameof(ResetTypeAsync), BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new InvalidOperationException("ResetTypeAsync not found.");

        private readonly Func<IRepositoryProvider> _ProviderAccessor;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a target. The provider is resolved lazily on every call, so the target can be built before the
        /// shared test database is initialized.
        /// </summary>
        /// <param name="name">Display name. Must not be null or empty.</param>
        /// <param name="providerAccessor">Returns the initialized provider. Must not be null.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null or empty.</exception>
        public SqlConformanceTarget(string name, Func<IRepositoryProvider> providerAccessor)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            Name = name;
            _ProviderAccessor = providerAccessor ?? throw new ArgumentNullException(nameof(providerAccessor));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Creates a SQL repository; options are copied into <see cref="SqlRepositoryOptions"/>.
        /// </summary>
        /// <typeparam name="T">Entity type.</typeparam>
        /// <param name="options">Options; null for defaults.</param>
        /// <returns>A repository owning its connection factory.</returns>
        public IRepository<T> CreateRepository<T>(RepositoryOptions? options = null) where T : class, new()
        {
            IRepositoryProvider provider = _ProviderAccessor();
            if (options == null) return provider.CreateRepository<T>();
            SqlRepositoryOptions sqlOptions = new SqlRepositoryOptions
            {
                Logger = options.Logger,
                LogParameterValues = options.LogParameterValues,
                StringMatching = options.StringMatching,
                SlowCommandThreshold = options.SlowCommandThreshold
            };
            return provider.CreateRepositoryWithOptions<T>(sqlOptions);
        }

        /// <summary>
        /// Drops and recreates the table of every given entity type.
        /// </summary>
        /// <param name="entityTypes">Entity types. Must not be null.</param>
        /// <param name="token">Cancellation token.</param>
        /// <returns>A task.</returns>
        /// <exception cref="ArgumentNullException">Thrown when entityTypes is null.</exception>
        public async Task ResetAsync(IReadOnlyList<Type> entityTypes, CancellationToken token = default)
        {
            ArgumentNullException.ThrowIfNull(entityTypes);
            IRepositoryProvider provider = _ProviderAccessor();
            foreach (Type type in entityTypes)
            {
                token.ThrowIfCancellationRequested();
                Task reset = (Task)_ResetTypeMethod.MakeGenericMethod(type).Invoke(this, new object[] { provider })!;
                await reset.ConfigureAwait(false);
            }
        }

        #endregion

        #region Private-Methods

        private async Task ResetTypeAsync<T>(IRepositoryProvider provider) where T : class, new()
        {
            using (ISqlRepository<T> repository = provider.CreateRepository<T>())
            {
                await RelTestHelpers.RecreateTableAsync(repository).ConfigureAwait(false);
            }
        }

        #endregion
    }
}
