namespace Durable
{
    using System;
    using System.Collections;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Builds compiled property accessors and constructors so hot paths avoid reflection.
    /// Thread safety: all members are stateless and thread-safe.
    /// </summary>
    public static class MemberAccessorFactory
    {
        /// <summary>
        /// Creates a compiled getter for a property.
        /// </summary>
        /// <param name="property">Property with a public getter. Must not be null.</param>
        /// <returns>A delegate taking the declaring instance (as object) and returning the boxed value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when property is null.</exception>
        public static Func<object, object?> CreateGetter(PropertyInfo property)
        {
            ArgumentNullException.ThrowIfNull(property);
            if (property.GetGetMethod(true) == null)
                return _ => null;

            ParameterExpression instance = Expression.Parameter(typeof(object), "instance");
            Expression typedInstance = Expression.Convert(instance, property.DeclaringType!);
            Expression access = Expression.Property(typedInstance, property);
            Expression boxed = Expression.Convert(access, typeof(object));
            return Expression.Lambda<Func<object, object?>>(boxed, instance).Compile();
        }

        /// <summary>
        /// Creates a compiled setter for a property. Null assigned to a non-nullable value type assigns the type's default.
        /// </summary>
        /// <param name="property">Property. Must not be null.</param>
        /// <returns>A delegate taking the declaring instance and the boxed value. A no-op when the property has no setter.</returns>
        /// <exception cref="ArgumentNullException">Thrown when property is null.</exception>
        public static Action<object, object?> CreateSetter(PropertyInfo property)
        {
            ArgumentNullException.ThrowIfNull(property);
            MethodInfo? setMethod = property.GetSetMethod(true);
            if (setMethod == null)
                return (_, _) => { };

            ParameterExpression instance = Expression.Parameter(typeof(object), "instance");
            ParameterExpression value = Expression.Parameter(typeof(object), "value");
            Expression typedInstance = Expression.Convert(instance, property.DeclaringType!);
            Type propertyType = property.PropertyType;

            Expression typedValue;
            if (propertyType.IsValueType && Nullable.GetUnderlyingType(propertyType) == null)
            {
                typedValue = Expression.Condition(
                    Expression.Equal(value, Expression.Constant(null)),
                    Expression.Default(propertyType),
                    Expression.Unbox(value, propertyType));
            }
            else
            {
                typedValue = Expression.Convert(value, propertyType);
            }

            Expression assign = Expression.Assign(Expression.Property(typedInstance, property), typedValue);
            return Expression.Lambda<Action<object, object?>>(assign, instance, value).Compile();
        }

        /// <summary>
        /// Creates a compiled parameterless constructor delegate.
        /// </summary>
        /// <param name="type">Type with a public or non-public parameterless constructor. Must not be null.</param>
        /// <returns>A factory delegate.</returns>
        /// <exception cref="ArgumentNullException">Thrown when type is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the type has no parameterless constructor.</exception>
        public static Func<object> CreateConstructor(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            if (type.IsValueType)
            {
                Expression boxedDefault = Expression.Convert(Expression.Default(type), typeof(object));
                return Expression.Lambda<Func<object>>(boxedDefault).Compile();
            }

            ConstructorInfo? ctor = type.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
            if (ctor == null)
                throw new InvalidOperationException("Type " + type.FullName + " has no parameterless constructor.");
            return Expression.Lambda<Func<object>>(Expression.New(ctor)).Compile();
        }

        /// <summary>
        /// Creates a compiled factory that builds an empty <see cref="System.Collections.Generic.List{T}"/> for the element type.
        /// </summary>
        /// <param name="elementType">Element type. Must not be null.</param>
        /// <returns>A factory delegate returning an <see cref="IList"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when elementType is null.</exception>
        public static Func<IList> CreateListFactory(Type elementType)
        {
            ArgumentNullException.ThrowIfNull(elementType);
            Type listType = typeof(System.Collections.Generic.List<>).MakeGenericType(elementType);
            Expression create = Expression.Convert(Expression.New(listType), typeof(IList));
            return Expression.Lambda<Func<IList>>(create).Compile();
        }
    }
}
