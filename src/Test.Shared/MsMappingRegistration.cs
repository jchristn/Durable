namespace Test.Shared
{
    using Durable;

    /// <summary>
    /// Registers <see cref="MapAttributeMappingSource"/> for the mapping-source test entities, once per process. Every test
    /// that touches those entities calls <see cref="EnsureRegistered"/> first, so registration always precedes the first
    /// metadata build no matter which suite or runner runs first.
    /// Thread safety: thread-safe.
    /// </summary>
    public static class MsMappingRegistration
    {
        #region Public-Members

        /// <summary>
        /// Gets the source registered for the per-type test entities. Never null.
        /// </summary>
        public static MapAttributeMappingSource Source { get; } = new MapAttributeMappingSource();

        /// <summary>
        /// Gets the source the global-registration test sets as <see cref="DurableMapping.MappingSource"/>; it describes only
        /// <see cref="MsGlobalItem"/>. Never null.
        /// </summary>
        public static MapAttributeMappingSource GlobalSource { get; } = new MapAttributeMappingSource(typeof(MsGlobalItem));

        #endregion

        #region Private-Members

        private static readonly object _Lock = new object();
        private static bool _Registered = false;

        #endregion

        #region Public-Methods

        /// <summary>
        /// Registers <see cref="Source"/> for the per-type test entities if that has not happened yet.
        /// </summary>
        public static void EnsureRegistered()
        {
            lock (_Lock)
            {
                if (_Registered) return;
                DurableMapping.Register<MsAuthor>(Source);
                DurableMapping.Register<MsBook>(Source);
                DurableMapping.Register<MsTag>(Source);
                DurableMapping.Register<MsAuthorTag>(Source);
                DurableMapping.Register<MsDocument>(Source);
                DurableMapping.Register<MsConventionItem>(Source);
                DurableMapping.Register<MsOverrideItem>(Source);
                _Registered = true;
            }
        }

        #endregion
    }
}
