namespace Test.Shared
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Durable.LiteGraph;

    /// <summary>
    /// A LiteGraph backend over a SQLite file in a private temporary directory, for one test: disposing it disposes the
    /// backend and deletes the directory.
    /// </summary>
    public sealed class LiteGraphTestStore : IAsyncDisposable
    {
        #region Public-Members

        /// <summary>
        /// Gets the backend. Never null.
        /// </summary>
        public LiteGraphBackend Backend { get; private set; }

        /// <summary>
        /// Gets the temporary directory. Never null.
        /// </summary>
        public string Directory { get; }

        /// <summary>
        /// Gets the database file path. Never null.
        /// </summary>
        public string Filename { get; }

        #endregion

        #region Constructors-and-Factories

        private LiteGraphTestStore(string directory, string filename, LiteGraphBackend backend)
        {
            Directory = directory;
            Filename = filename;
            Backend = backend;
        }

        /// <summary>
        /// Creates a store over a new database file.
        /// </summary>
        /// <param name="configure">Optional settings customization (the filename is already set).</param>
        /// <returns>The store.</returns>
        public static async Task<LiteGraphTestStore> CreateAsync(Action<LiteGraphBackendSettings>? configure = null)
        {
            string directory = Path.Combine(Path.GetTempPath(), "durable-litegraph-test-" + Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(directory);
            string filename = Path.Combine(directory, "graph.db");
            LiteGraphBackendSettings settings = LiteGraphBackendSettings.ForFile(filename);
            configure?.Invoke(settings);
            return new LiteGraphTestStore(directory, filename, await LiteGraphBackend.CreateAsync(settings));
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Disposes the backend and opens a new one over the same file (same tenant and graph names).
        /// </summary>
        /// <param name="configure">Optional settings customization (the filename is already set).</param>
        /// <returns>A task.</returns>
        public async Task ReopenAsync(Action<LiteGraphBackendSettings>? configure = null)
        {
            await Backend.DisposeAsync();
            LiteGraphBackendSettings settings = LiteGraphBackendSettings.ForFile(Filename);
            configure?.Invoke(settings);
            Backend = await LiteGraphBackend.CreateAsync(settings);
        }

        /// <summary>
        /// Disposes the backend and deletes the directory.
        /// </summary>
        /// <returns>A task.</returns>
        public async ValueTask DisposeAsync()
        {
            await Backend.DisposeAsync();
            try
            {
                System.IO.Directory.Delete(Directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        #endregion
    }
}
