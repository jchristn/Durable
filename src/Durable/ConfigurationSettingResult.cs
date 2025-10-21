namespace Durable
{
    /// <summary>
    /// Represents the result of resolving a configuration setting.
    /// </summary>
    public sealed class ConfigurationSettingResult
    {
        /// <summary>
        /// Gets or sets the effective setting value.
        /// </summary>
        public bool EffectiveSetting { get; set; }

        /// <summary>
        /// Gets or sets the source of the effective setting (e.g., "Instance", "ThreadLocal", "Global").
        /// </summary>
        public string Source { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigurationSettingResult"/> class.
        /// </summary>
        public ConfigurationSettingResult()
        {
            Source = string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfigurationSettingResult"/> class.
        /// </summary>
        /// <param name="effectiveSetting">The effective setting value.</param>
        /// <param name="source">The source of the setting.</param>
        public ConfigurationSettingResult(bool effectiveSetting, string source)
        {
            EffectiveSetting = effectiveSetting;
            Source = source;
        }
    }
}
