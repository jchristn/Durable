namespace Durable.InMemory
{
    using System;
    using System.Text.Json;
    using Durable;

    /// <summary>
    /// Settings for an <see cref="InMemoryBackend"/>: the capabilities it advertises and how JSON columns are stored. An
    /// in-memory backend always holds its data in process memory, discarded when the backend is disposed or collected.
    /// Thread safety: not thread-safe; configure before creating the backend. The backend reads the settings once, when it
    /// is created; later changes have no effect on it.
    /// </summary>
    public sealed class InMemoryRepositorySettings
    {
        #region Public-Members

        /// <summary>
        /// Gets whether the data is held only in memory and discarded on dispose. Always true for this backend; present so
        /// every backend's settings answer the same question.
        /// </summary>
        public bool IsInMemory => true;

        /// <summary>
        /// Gets or sets the optional features the backend advertises. Clear flags to simulate a limited backend in tests;
        /// repositories then reject the masked features with <see cref="NotSupportedException"/>.
        /// Default: <see cref="RepositoryCapabilities.All"/>.
        /// </summary>
        public RepositoryCapabilities Capabilities { get; set; } = RepositoryCapabilities.All;

        /// <summary>
        /// Gets or sets the JSON options used to store JSON columns; null for camelCase, non-indented output (the same as
        /// the SQL providers). Under Native AOT, pass options with a source-generated context
        /// (<see cref="DurableJson.CreateOptions"/>).
        /// Default: null.
        /// </summary>
        public JsonSerializerOptions? JsonOptions { get; set; }

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates settings with defaults (all capabilities, default JSON options).
        /// </summary>
        public InMemoryRepositorySettings()
        {
        }

        /// <summary>
        /// Creates settings with defaults. Equivalent to the constructor; present so every backend's settings have the same
        /// factory for an ephemeral in-memory store.
        /// </summary>
        /// <returns>The settings. Never null.</returns>
        public static InMemoryRepositorySettings ForInMemory()
        {
            return new InMemoryRepositorySettings();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Validates the settings. <see cref="InMemoryBackend.Create"/> calls it.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when <see cref="Capabilities"/> contains undefined flags.</exception>
        public void Validate()
        {
            if ((Capabilities & ~RepositoryCapabilities.All) != 0)
                throw new ArgumentException("Capabilities contains undefined flags: " + Capabilities + ".");
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "In-memory (" + Capabilities + ")";
        }

        #endregion
    }
}
