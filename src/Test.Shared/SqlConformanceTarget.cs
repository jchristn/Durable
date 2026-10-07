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
    /// SQL repositories support <see cref="RepositoryCapabilities.All"/>, except <see cref="RepositoryCapabilities.EmptyStrings"/>
    /// on a dialect that stores empty strings as NULL (Oracle); the capabilities are passed in because the kit reads them
    /// before the provider is initialized, and every repository created is checked against them.
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
        /// Gets the capabilities given to the constructor (default <see cref="RepositoryCapabilities.All"/>).
        /// </summary>
        public RepositoryCapabilities Capabilities { get; }

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
        /// <param name="capabilities">Capabilities of the provider's repositories. Default: <see cref="RepositoryCapabilities.All"/>.</param>
        /// <exception cref="ArgumentNullException">Thrown when an argument is null or empty.</exception>
        public SqlConformanceTarget(string name, Func<IRepositoryProvider> providerAccessor, RepositoryCapabilities capabilities = RepositoryCapabilities.All)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            Name = name;
            _ProviderAccessor = providerAccessor ?? throw new ArgumentNullException(nameof(providerAccessor));
            Capabilities = capabilities;
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
            ISqlRepository<T> repository;
            if (options == null)
            {
                repository = provider.CreateRepository<T>();
            }
            else
            {
                SqlRepositoryOptions sqlOptions = new SqlRepositoryOptions
                {
                    Logger = options.Logger,
                    LogParameterValues = options.LogParameterValues,
                    StringMatching = options.StringMatching,
                    SlowCommandThreshold = options.SlowCommandThreshold
                };
                repository = provider.CreateRepositoryWithOptions<T>(sqlOptions);
            }

            if (repository.Capabilities != Capabilities)
            {
                repository.Dispose();
                throw new InvalidOperationException("The " + Name + " repository reports " + repository.Capabilities + " but the conformance target declares " + Capabilities + ".");
            }

            return repository;
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
