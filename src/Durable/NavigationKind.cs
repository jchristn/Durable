namespace Durable
{
    /// <summary>
    /// The shape of a navigation property.
    /// </summary>
    public enum NavigationKind
    {
        /// <summary>
        /// Many-to-one or one-to-one: this entity holds the foreign key (<see cref="NavigationPropertyAttribute"/>).
        /// </summary>
        Reference = 0,

        /// <summary>
        /// One-to-many: the related entity holds a foreign key to this entity (<see cref="InverseNavigationPropertyAttribute"/>).
        /// </summary>
        Collection = 1,

        /// <summary>
        /// Many-to-many through a junction entity (<see cref="ManyToManyNavigationPropertyAttribute"/>).
        /// </summary>
        ManyToMany = 2
    }
}
