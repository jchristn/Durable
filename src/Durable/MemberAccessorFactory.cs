namespace Durable
{
    using System;
    using System.Collections;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// Builds fast property accessors and constructors so hot paths avoid late-bound reflection.
    /// When dynamic code is supported (JIT), accessors are compiled expression trees. Under Native AOT
    /// (<see cref="RuntimeFeature.IsDynamicCodeSupported"/> is false), where compiled expressions would run on the
    /// expression interpreter, accessors use <see cref="MethodInvoker"/>/<see cref="ConstructorInvoker"/>, which call
    /// the ahead-of-time generated invoke stubs directly.
    /// Thread safety: all members are stateless and thread-safe.
    /// </summary>
    public static class MemberAccessorFactory
    {
        /// <summary>
        /// The members <see cref="CreateConstructor(Type)"/> needs on a type: its public parameterless constructor and its
        /// non-public constructors. Use it to annotate <see cref="Type"/> parameters and generic parameters that flow into
        /// <see cref="CreateConstructor(Type)"/> so trimming and Native AOT keep the constructor.
        /// </summary>
        public const DynamicallyAccessedMemberTypes ConstructorMemberTypes =
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.NonPublicConstructors;

        /// <summary>
        /// Creates a fast getter for a property.
        /// </summary>
        /// <param name="property">Property with a public getter. Must not be null.</param>
        /// <returns>A delegate taking the declaring instance (as object) and returning the boxed value.</returns>
        /// <exception cref="ArgumentNullException">Thrown when property is null.</exception>
        public static Func<object, object?> CreateGetter(PropertyInfo property)
        {
            ArgumentNullException.ThrowIfNull(property);
            MethodInfo? getMethod = property.GetGetMethod(true);
            if (getMethod == null)
                return _ => null;

            if (!RuntimeFeature.IsDynamicCodeSupported)
            {
                MethodInvoker invoker = MethodInvoker.Create(getMethod);
                return instance => invoker.Invoke(instance);
            }

            ParameterExpression instanceParameter = Expression.Parameter(typeof(object), "instance");
            Expression typedInstance = Expression.Convert(instanceParameter, property.DeclaringType!);
            Expression access = Expression.Property(typedInstance, property);
            Expression boxed = Expression.Convert(access, typeof(object));
            return Expression.Lambda<Func<object, object?>>(boxed, instanceParameter).Compile();
        }

        /// <summary>
        /// Creates a fast setter for a property. Null assigned to a non-nullable value type assigns the type's default.
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

            Type propertyType = property.PropertyType;
            bool nonNullableValueType = propertyType.IsValueType && Nullable.GetUnderlyingType(propertyType) == null;

            if (!RuntimeFeature.IsDynamicCodeSupported)
            {
                MethodInvoker invoker = MethodInvoker.Create(setMethod);
                if (!nonNullableValueType) return (instance, value) => invoker.Invoke(instance, value);
                object defaultValue = GetDefaultValue(propertyType)!;
                return (instance, value) => invoker.Invoke(instance, value ?? defaultValue);
            }

            ParameterExpression instanceParameter = Expression.Parameter(typeof(object), "instance");
            ParameterExpression valueParameter = Expression.Parameter(typeof(object), "value");
            Expression typedInstance = Expression.Convert(instanceParameter, property.DeclaringType!);

            Expression typedValue;
            if (nonNullableValueType)
            {
                typedValue = Expression.Condition(
                    Expression.Equal(valueParameter, Expression.Constant(null)),
                    Expression.Default(propertyType),
                    Expression.Unbox(valueParameter, propertyType));
            }
            else
            {
                typedValue = Expression.Convert(valueParameter, propertyType);
            }

            Expression assign = Expression.Assign(Expression.Property(typedInstance, property), typedValue);
            return Expression.Lambda<Action<object, object?>>(assign, instanceParameter, valueParameter).Compile();
        }

        /// <summary>
        /// Creates a fast parameterless constructor delegate.
        /// </summary>
        /// <param name="type">Type with a public or non-public parameterless constructor. Must not be null.</param>
        /// <returns>A factory delegate.</returns>
        /// <exception cref="ArgumentNullException">Thrown when type is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the type has no parameterless constructor.</exception>
        public static Func<object> CreateConstructor([DynamicallyAccessedMembers(ConstructorMemberTypes)] Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            if (type.IsValueType)
                return () => GetDefaultValue(type)!;

            ConstructorInfo? ctor = type.GetConstructor(Type.EmptyTypes)
                ?? type.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
            if (ctor == null)
                throw new InvalidOperationException("Type " + type.FullName + " has no parameterless constructor.");

            if (!RuntimeFeature.IsDynamicCodeSupported)
            {
                ConstructorInvoker invoker = ConstructorInvoker.Create(ctor);
                return () => invoker.Invoke();
            }

            return Expression.Lambda<Func<object>>(Expression.New(ctor)).Compile();
        }

        /// <summary>
        /// Creates a factory that builds an empty <see cref="System.Collections.Generic.List{T}"/> for the element type.
        /// </summary>
        /// <param name="elementType">Element type. Must not be null. Under Native AOT it must be a reference type
        /// (collection navigations always target entity classes).</param>
        /// <returns>A factory delegate returning an <see cref="IList"/>.</returns>
        /// <exception cref="ArgumentNullException">Thrown when elementType is null.</exception>
        /// <exception cref="NotSupportedException">Thrown under Native AOT when elementType is a value type.</exception>
        public static Func<IList> CreateListFactory(Type elementType)
        {
            ArgumentNullException.ThrowIfNull(elementType);
            if (!RuntimeFeature.IsDynamicCodeSupported && elementType.IsValueType)
                throw new NotSupportedException("Collection navigations over value type " + elementType.Name + " are not supported under Native AOT.");

            Type listType = MakeReferenceListType(elementType);
            if (!RuntimeFeature.IsDynamicCodeSupported)
            {
                ConstructorInvoker invoker = ConstructorInvoker.Create(GetListConstructor(listType));
                return () => (IList)invoker.Invoke();
            }

            Expression create = Expression.Convert(Expression.New(listType), typeof(IList));
            return Expression.Lambda<Func<IList>>(create).Compile();
        }

        /// <summary>
        /// Returns the default value of a type: null for reference types and nullable value types, otherwise the boxed
        /// all-zero value. Never runs a constructor.
        /// </summary>
        /// <param name="type">Type. Must not be null.</param>
        /// <returns>The default value, or null.</returns>
        /// <exception cref="ArgumentNullException">Thrown when type is null.</exception>
        [UnconditionalSuppressMessage("Trimming", "IL2067", Justification = "GetUninitializedObject is only called for value types. A value type's default instance is created without running any constructor, so the constructor members its annotation requests are never used; the boxed type itself is present because the caller already holds a value or member of that type.")]
        public static object? GetDefaultValue(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            if (!type.IsValueType || Nullable.GetUnderlyingType(type) != null) return null;
            return RuntimeHelpers.GetUninitializedObject(type);
        }

        [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Only reached for reference element types under Native AOT (checked by the caller). List<T> over reference types shares the canonical List<__Canon> code that is always compiled, so the runtime can construct the instantiation without new native code.")]
        [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
        [UnconditionalSuppressMessage("Trimming", "IL2073", Justification = "The returned type is a List<T> instantiation; List<T>'s public parameterless constructor is part of the framework and kept because List<T> is used throughout the library.")]
        private static Type MakeReferenceListType(Type elementType)
        {
            return typeof(System.Collections.Generic.List<>).MakeGenericType(elementType);
        }

        private static ConstructorInfo GetListConstructor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] Type listType)
        {
            return listType.GetConstructor(Type.EmptyTypes)!;
        }
    }
}
