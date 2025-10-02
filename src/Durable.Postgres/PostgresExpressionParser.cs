namespace Durable.Postgres
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Text;

    /// <summary>
    /// Parses and converts LINQ expressions to PostgreSQL-compatible SQL strings.
    /// Provides support for complex expression trees including binary operations, method calls, and member access.
    /// </summary>
    /// <typeparam name="T">The entity type that the expressions operate on.</typeparam>
    public class PostgresExpressionParser<T> where T : class
    {

        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Dictionary<string, PropertyInfo> _ColumnMappings;
        private readonly ISanitizer _Sanitizer;
        private readonly List<(string name, object? value)> _Parameters;
        private bool _UseParameterizedQueries;

        // Type-specific static cache for compiled expressions to avoid repeated compilation overhead
        // IMPORTANT: This static field is unique per generic type T (e.g., PostgresExpressionParser<User> has
        // a separate cache from PostgresExpressionParser<Product>), preventing cross-type pollution
        // Using ConditionalWeakTable to allow garbage collection of both keys and values
        private static readonly ConditionalWeakTable<Expression, Func<object?>> _CompiledExpressions = new ConditionalWeakTable<Expression, Func<object?>>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the PostgresExpressionParser with the specified column mappings and sanitizer.
        /// </summary>
        /// <param name="columnMappings">A dictionary mapping property names to their corresponding database column names and PropertyInfo objects.</param>
        /// <param name="sanitizer">The sanitizer to use for value formatting and SQL injection prevention. Defaults to PostgresSanitizer if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when columnMappings is null.</exception>
        public PostgresExpressionParser(Dictionary<string, PropertyInfo> columnMappings, ISanitizer? sanitizer = null)
        {
            _ColumnMappings = columnMappings ?? throw new ArgumentNullException(nameof(columnMappings));
            _Sanitizer = sanitizer ?? new PostgresSanitizer();
            _Parameters = new List<(string name, object? value)>();
            _UseParameterizedQueries = false;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Parses any expression tree and converts it to its PostgreSQL SQL equivalent string representation.
        /// </summary>
        /// <param name="expression">The expression tree to parse and convert to SQL.</param>
        /// <returns>A string containing the PostgreSQL-compatible SQL representation of the expression.</returns>
        /// <exception cref="NotSupportedException">Thrown when an unsupported expression type is encountered.</exception>
        public string ParseExpression(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));
            return Visit(expression);
        }

        /// <summary>
        /// Parses an expression tree and converts it to parameterized PostgreSQL SQL with extracted parameters.
        /// This method clears any existing parameters before parsing.
        /// </summary>
        /// <param name="expression">The expression tree to parse and convert to SQL.</param>
        /// <param name="useParameterizedQueries">If true, extracts values as parameters; if false, embeds values directly (for backward compatibility).</param>
        /// <returns>A string containing the PostgreSQL-compatible SQL representation of the expression.</returns>
        /// <exception cref="NotSupportedException">Thrown when an unsupported expression type is encountered.</exception>
        public string ParseExpressionWithParameters(Expression expression, bool useParameterizedQueries = true)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            // Clear existing parameters for fresh parsing
            _Parameters.Clear();
            _UseParameterizedQueries = useParameterizedQueries;

            return Visit(expression);
        }

        /// <summary>
        /// Gets the parameters that were collected during the last call to ParseExpressionWithParameters.
        /// </summary>
        /// <returns>A list of parameter name-value pairs extracted from the expression.</returns>
        public List<(string name, object? value)> GetParameters()
        {
            return new List<(string name, object? value)>(_Parameters);
        }

        /// <summary>
        /// Extracts the database column name from a member expression that references an entity property.
        /// </summary>
        /// <param name="expression">The member expression representing a property access (e.g., p.FirstName).</param>
        /// <returns>The corresponding database column name for the property.</returns>
        /// <exception cref="ArgumentException">Thrown when the expression is not a valid member expression or the property is not mapped to a column.</exception>
        public string GetColumnFromExpression(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));
            if (expression is MemberExpression memberExpr)
            {
                PropertyInfo? propInfo = memberExpr.Member as PropertyInfo;
                if (propInfo != null)
                {
                    KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                    if (mapping.Key != null)
                        return $"\"{mapping.Key}\""; // PostgreSQL uses double quotes for identifiers
                }
            }
            throw new ArgumentException($"Expression is not a valid member expression or property is not mapped to a column: {expression}", nameof(expression));
        }

        /// <summary>
        /// Gets the raw column name without quotes from a member expression.
        /// Used internally when quotes will be added manually.
        /// </summary>
        /// <param name="expression">The member expression representing a property access.</param>
        /// <returns>The raw column name without quotes.</returns>
        /// <exception cref="ArgumentException">Thrown when the expression is not a valid member expression or the property is not mapped to a column.</exception>
        private string GetRawColumnFromExpression(Expression expression)
        {
            if (expression is MemberExpression memberExpr)
            {
                PropertyInfo? propInfo = memberExpr.Member as PropertyInfo;
                if (propInfo != null)
                {
                    KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                    if (mapping.Key != null)
                        return mapping.Key;
                }
            }
            throw new ArgumentException($"Expression is not a valid member expression or property is not mapped to a column: {expression}", nameof(expression));
        }

        /// <summary>
        /// Parses an update expression that specifies how to modify entity properties and converts it to SQL SET clause format.
        /// </summary>
        /// <param name="updateExpression">A lambda expression defining the property updates using member initialization syntax (e.g., p => new Person { Name = "John", Age = 30 }).</param>
        /// <returns>A string containing the SQL SET clause with column assignments (e.g., "\"Name\" = 'John', \"Age\" = 30").</returns>
        /// <exception cref="ArgumentException">Thrown when the expression is not a member initialization expression.</exception>
        /// <exception cref="NotSupportedException">Thrown when an unsupported expression type is encountered in the update values.</exception>
        public string ParseUpdateExpression(Expression<Func<T, T>> updateExpression)
        {
            if (updateExpression == null) throw new ArgumentNullException(nameof(updateExpression));
            if (updateExpression.Body is MemberInitExpression memberInit)
            {
                List<string> setPairs = new List<string>();
                foreach (MemberBinding binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                    {
                        PropertyInfo? propInfo = assignment.Member as PropertyInfo;
                        KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                        if (mapping.Key != null)
                        {
                            string columnName = mapping.Key; // Keep raw for manual quote control
                            string valueExpression = ParseUpdateValue(assignment.Expression);
                            setPairs.Add($"\"{columnName}\" = {valueExpression}");
                        }
                    }
                }
                return string.Join(", ", setPairs);
            }
            throw new ArgumentException("Update expression must be a member initialization expression (e.g., p => new Person { Name = \"John\", Age = 30 })", nameof(updateExpression));
        }

        /// <summary>
        /// Gets the count of cached compiled expressions for this specific T type.
        /// This method is useful for testing and verifying cache isolation between different entity types.
        /// </summary>
        /// <returns>The number of expressions currently cached for this type T</returns>
        public static int GetCacheCount()
        {
            return _CompiledExpressions.Count();
        }

        /// <summary>
        /// Clears the expression cache for this specific T type.
        /// This method is useful for testing and debugging cache behavior.
        /// </summary>
        public static void ClearCache()
        {
            _CompiledExpressions.Clear();
        }

        #endregion

        #region Private-Methods

        private string Visit(Expression expression)
        {
            switch (expression)
            {
                case BinaryExpression binary:
                    return VisitBinary(binary);
                case MemberExpression member:
                    return VisitMember(member);
                case ConstantExpression constant:
                    return VisitConstant(constant);
                case MethodCallExpression methodCall:
                    return VisitMethodCall(methodCall);
                case UnaryExpression unary:
                    return VisitUnary(unary);
                case ConditionalExpression conditional:
                    return VisitConditional(conditional);
                case NewArrayExpression newArray:
                    return VisitNewArray(newArray);
                default:
                    throw new NotSupportedException($"Expression type '{expression.GetType().Name}' is not supported in LINQ expressions. Expression: {expression}");
            }
        }

        private string VisitBinary(BinaryExpression binary)
        {
            string left = VisitWithPrecedence(binary.Left, binary.NodeType, true);
            string right = VisitWithPrecedence(binary.Right, binary.NodeType, false);

            string op = binary.NodeType switch
            {
                // Comparison operators
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "!=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",

                // Logical operators
                ExpressionType.AndAlso => "AND",
                ExpressionType.OrElse => "OR",
                ExpressionType.And => "AND",
                ExpressionType.Or => "OR",

                // Math operators
                ExpressionType.Add => "+",
                ExpressionType.Subtract => "-",
                ExpressionType.Multiply => "*",
                ExpressionType.Divide => "/",
                ExpressionType.Modulo => "%",

                // Coalesce operator - handled specially below
                ExpressionType.Coalesce => "COALESCE",

                _ => throw new NotSupportedException($"Binary operator '{binary.NodeType}' is not supported in PostgreSQL expressions")
            };

            // Handle null comparisons
            if (right == "NULL")
            {
                if (op == "=") return $"{left} IS NULL";
                if (op == "!=") return $"{left} IS NOT NULL";
            }

            // Handle null coalescing operator (PostgreSQL uses COALESCE function)
            if (binary.NodeType == ExpressionType.Coalesce)
            {
                return $"COALESCE({left}, {right})";
            }

            // Handle string concatenation (PostgreSQL uses || operator)
            if (binary.NodeType == ExpressionType.Add &&
                (binary.Left.Type == typeof(string) || binary.Right.Type == typeof(string)))
            {
                return $"({left} || {right})";
            }

            return $"{left} {op} {right}";
        }

        private string VisitMember(MemberExpression member)
        {
            // Check if this is a property access on the entity parameter (e.g., p.FirstName)
            if (member.Expression is ParameterExpression && member.Member is PropertyInfo propInfo)
            {
                KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                if (mapping.Key != null)
                    return $"\"{mapping.Key}\""; // PostgreSQL uses double quotes
            }

            // Special handling for Length property access on method call results (e.g., p.FirstName.Trim().Length)
            if (member.Member.Name == "Length" && member.Expression is MethodCallExpression methodCall &&
                member.Member.DeclaringType == typeof(string))
            {
                // Handle as LENGTH(method_result) for PostgreSQL
                string methodResult = Visit(methodCall);
                return $"LENGTH({methodResult})";
            }

            // Special handling for Length property access on direct properties (e.g., p.FirstName.Length)
            if (member.Member.Name == "Length" && member.Expression is MemberExpression propertyMember &&
                member.Member.DeclaringType == typeof(string))
            {
                // Check if this is a direct property access (e.g., p.FirstName.Length)
                if (propertyMember.Expression is ParameterExpression && propertyMember.Member is PropertyInfo lengthPropInfo)
                {
                    KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == lengthPropInfo);
                    if (mapping.Key != null)
                        return $"LENGTH(\"{mapping.Key}\")";
                }
            }

            // Handle navigation property chains (e.g., b.Author.Name)
            if (member.Expression is MemberExpression navigationMember && member.Member is PropertyInfo targetProperty)
            {
                // Try to resolve the navigation property chain to a table alias and column
                string? columnReference = ResolveNavigationPropertyChain(member);
                if (columnReference != null)
                    return columnReference;
            }

            // Check if this member expression ultimately references the entity parameter through method calls
            if (ContainsParameterReference(member))
            {
                // This is likely a property access result that should be treated as SQL expression
                // For other cases not handled above, we might need to extend this logic
                throw new NotSupportedException($"Complex member access pattern '{member}' is not supported. Only direct property access (e.g., p.Name) is currently supported.");
            }

            // Handle constant member access or property chains
            object? value = GetMemberValue(member);
            return FormatValue(value);
        }

        private string VisitConstant(ConstantExpression constant)
        {
            if (constant.Value is DateTime dateTime)
            {
                double timeDiff = Math.Abs((DateTime.Now - dateTime).TotalSeconds);
                if (timeDiff < 5)
                {
                    return "NOW()";
                }
            }
            return FormatValue(constant.Value);
        }

        private string VisitMethodCall(MethodCallExpression methodCall)
        {
            switch (methodCall.Method.Name)
            {
                case "Equals":
                    // Handle string.Equals(value) or value.Equals(string)
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        string left = Visit(methodCall.Object);
                        string right = Visit(methodCall.Arguments[0]);
                        return $"{left} = {right}";
                    }
                    else if (methodCall.Arguments.Count == 2)
                    {
                        // Static Equals method
                        string left = Visit(methodCall.Arguments[0]);
                        string right = Visit(methodCall.Arguments[1]);
                        return $"{left} = {right}";
                    }
                    break;

                case "Contains":
                    return HandleContains(methodCall);

                case "StartsWith":
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        string column = Visit(methodCall.Object);
                        object? value = GetConstantValue(methodCall.Arguments[0]);
                        string sanitizedValue = _Sanitizer.SanitizeLikeValue(value?.ToString() ?? "");
                        // Remove quotes and add % at the end
                        string innerValue = sanitizedValue.Trim('\'');
                        return $"{column} LIKE '{innerValue}%'";
                    }
                    break;

                case "EndsWith":
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        string column = Visit(methodCall.Object);
                        object? value = GetConstantValue(methodCall.Arguments[0]);
                        string sanitizedValue = _Sanitizer.SanitizeLikeValue(value?.ToString() ?? "");
                        // Remove quotes and add % at the beginning
                        string innerValue = sanitizedValue.Trim('\'');
                        return $"{column} LIKE '%{innerValue}'";
                    }
                    break;

                // DateTime operations - PostgreSQL specific functions
                case "AddDays":
                case "AddHours":
                case "AddMinutes":
                case "AddSeconds":
                case "AddMonths":
                case "AddYears":
                    return HandleDateTimeAdd(methodCall);

                case "get_Year":
                case "get_Month":
                case "get_Day":
                case "get_Hour":
                case "get_Minute":
                case "get_Second":
                    return HandleDateTimePart(methodCall);

                // Math operations
                case "Abs":
                case "Floor":
                case "Ceiling":
                case "Round":
                case "Sqrt":
                case "Sin":
                case "Cos":
                case "Tan":
                    return HandleMathFunction(methodCall);

                default:
                    throw new NotSupportedException($"Method '{methodCall.Method.Name}' is not supported in PostgreSQL expressions");
            }
            throw new NotSupportedException($"Method call pattern '{methodCall}' is not supported");
        }

        private string VisitUnary(UnaryExpression unary)
        {
            switch (unary.NodeType)
            {
                case ExpressionType.Not:
                    string operand = Visit(unary.Operand);
                    return $"NOT ({operand})";

                case ExpressionType.Negate:
                    return $"-{Visit(unary.Operand)}";

                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    // Most type conversions in LINQ don't need explicit casting in PostgreSQL
                    return Visit(unary.Operand);

                default:
                    throw new NotSupportedException($"Unary operator '{unary.NodeType}' is not supported");
            }
        }

        private string VisitConditional(ConditionalExpression conditional)
        {
            string test = Visit(conditional.Test);
            string ifTrue = Visit(conditional.IfTrue);
            string ifFalse = Visit(conditional.IfFalse);
            return $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
        }

        private string VisitNewArray(NewArrayExpression newArray)
        {
            if (newArray.Expressions.Count == 0)
                return "ARRAY[]::text[]"; // Empty PostgreSQL array

            List<string> elements = newArray.Expressions.Select(Visit).ToList();
            return $"ARRAY[{string.Join(", ", elements)}]";
        }

        // Placeholder methods - these would need full implementation similar to MySQL version
        private string VisitWithPrecedence(Expression expression, ExpressionType parentType, bool isLeft) => Visit(expression);
        private string? ResolveNavigationPropertyChain(MemberExpression member) => null;
        private bool ContainsParameterReference(MemberExpression member) => false;
        private object? GetMemberValue(MemberExpression member) => null;
        private string FormatValue(object? value) => _Sanitizer.FormatValue(value!);
        private string ParseUpdateValue(Expression expression) => Visit(expression);
        private string HandleContains(MethodCallExpression methodCall) => throw new NotImplementedException();
        private string HandleDateTimeAdd(MethodCallExpression methodCall) => throw new NotImplementedException();
        private string HandleDateTimePart(MethodCallExpression methodCall) => throw new NotImplementedException();
        private string HandleMathFunction(MethodCallExpression methodCall) => throw new NotImplementedException();
        private object? GetConstantValue(Expression expression) => null;

        #endregion
    }
}