namespace Durable.MySql
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;

    /// <summary>
    /// Parses and converts LINQ expressions to MySQL-compatible SQL strings.
    /// Provides support for complex expression trees including binary operations, method calls, and member access.
    /// </summary>
    /// <typeparam name="T">The entity type that the expressions operate on.</typeparam>
    public class MySqlExpressionParser<T> where T : class
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Dictionary<string, PropertyInfo> _ColumnMappings;
        private readonly ISanitizer _Sanitizer;
        private readonly List<(string name, object? value)> _Parameters;
        private int _ParameterCounter;
        private bool _UseParameterizedQueries;

        // Navigation property to table alias mapping for Include operations
        // Maps navigation property names (e.g., "Author", "Company") to their SQL table aliases (e.g., "t1", "t2")
        private Dictionary<string, string> _NavigationPropertyAliases;

        // Type-specific static cache for compiled expressions to avoid repeated compilation overhead
        // IMPORTANT: This static field is unique per generic type T (e.g., MySqlExpressionParser<User> has
        // a separate cache from MySqlExpressionParser<Product>), preventing cross-type pollution
        // Using ConcurrentDictionary with structural equality comparer to enable cache hits for semantically
        // identical expressions (e.g., p => p.Name == "John" called twice will reuse the same compiled delegate)
        private static readonly ConcurrentDictionary<Expression, Func<object?>> _CompiledExpressions =
            new ConcurrentDictionary<Expression, Func<object?>>(new ExpressionStructuralEqualityComparer());

        // Cache effectiveness metrics for monitoring and testing
        private static long _CacheHits = 0;
        private static long _CacheMisses = 0;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the MySqlExpressionParser with the specified column mappings and sanitizer.
        /// </summary>
        /// <param name="columnMappings">A dictionary mapping property names to their corresponding database column names and PropertyInfo objects.</param>
        /// <param name="sanitizer">The sanitizer to use for value formatting and SQL injection prevention. Defaults to MySqlSanitizer if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when columnMappings is null.</exception>
        public MySqlExpressionParser(Dictionary<string, PropertyInfo> columnMappings, ISanitizer? sanitizer = null)
        {
            _ColumnMappings = columnMappings ?? throw new ArgumentNullException(nameof(columnMappings));
            _Sanitizer = sanitizer ?? new MySqlSanitizer();
            _Parameters = new List<(string name, object? value)>();
            _ParameterCounter = 0;
            _UseParameterizedQueries = false;
            _NavigationPropertyAliases = new Dictionary<string, string>();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Parses any expression tree and converts it to its MySQL SQL equivalent string representation.
        /// </summary>
        /// <param name="expression">The expression tree to parse and convert to SQL.</param>
        /// <returns>A string containing the MySQL-compatible SQL representation of the expression.</returns>
        /// <exception cref="NotSupportedException">Thrown when an unsupported expression type is encountered.</exception>
        public string ParseExpression(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));
            return Visit(expression);
        }

        /// <summary>
        /// Parses an expression tree and converts it to parameterized MySQL SQL with extracted parameters.
        /// This method clears any existing parameters before parsing.
        /// </summary>
        /// <param name="expression">The expression tree to parse and convert to SQL.</param>
        /// <param name="useParameterizedQueries">If true, extracts values as parameters; if false, embeds values directly (for backward compatibility).</param>
        /// <returns>A string containing the MySQL-compatible SQL representation of the expression.</returns>
        /// <exception cref="NotSupportedException">Thrown when an unsupported expression type is encountered.</exception>
        public string ParseExpressionWithParameters(Expression expression, bool useParameterizedQueries = true)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            // Clear existing parameters for fresh parsing
            _Parameters.Clear();
            _ParameterCounter = 0;
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
                        return $"`{mapping.Key}`";
                }
            }
            throw new ArgumentException($"Expression is not a valid member expression or property is not mapped to a column: {expression}", nameof(expression));
        }

        /// <summary>
        /// Gets the raw column name without backticks from a member expression.
        /// Used internally when backticks will be added manually.
        /// </summary>
        /// <param name="expression">The member expression representing a property access.</param>
        /// <returns>The raw column name without backticks.</returns>
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
        /// <returns>A string containing the SQL SET clause with column assignments (e.g., "`Name` = 'John', `Age` = 30").</returns>
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
                            string columnName = mapping.Key; // Keep raw for manual backtick control
                            string valueExpression = ParseUpdateValue(assignment.Expression);
                            setPairs.Add($"`{columnName}` = {valueExpression}");
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
            return _CompiledExpressions.Count;
        }

        /// <summary>
        /// Clears the expression cache for this specific T type.
        /// This method is useful for testing and debugging cache behavior.
        /// </summary>
        public static void ClearCache()
        {
            _CompiledExpressions.Clear();
            System.Threading.Interlocked.Exchange(ref _CacheHits, 0);
            System.Threading.Interlocked.Exchange(ref _CacheMisses, 0);
        }

        /// <summary>
        /// Gets the cache hit count for this specific T type.
        /// This metric indicates how many times a compiled expression was retrieved from cache.
        /// </summary>
        /// <returns>The number of cache hits since the last cache clear</returns>
        public static long GetCacheHitCount()
        {
            return System.Threading.Interlocked.Read(ref _CacheHits);
        }

        /// <summary>
        /// Gets the cache miss count for this specific T type.
        /// This metric indicates how many times an expression had to be compiled and cached.
        /// </summary>
        /// <returns>The number of cache misses since the last cache clear</returns>
        public static long GetCacheMissCount()
        {
            return System.Threading.Interlocked.Read(ref _CacheMisses);
        }

        /// <summary>
        /// Gets the cache hit rate as a percentage for this specific T type.
        /// A higher rate indicates better cache effectiveness.
        /// </summary>
        /// <returns>The cache hit rate as a percentage (0-100), or 0 if no cache operations have occurred</returns>
        public static double GetCacheHitRate()
        {
            long hits = System.Threading.Interlocked.Read(ref _CacheHits);
            long misses = System.Threading.Interlocked.Read(ref _CacheMisses);
            long total = hits + misses;

            if (total == 0)
                return 0.0;

            return (hits / (double)total) * 100.0;
        }

        /// <summary>
        /// Sets the navigation property to table alias mappings for Include operations.
        /// This allows the expression parser to correctly resolve navigation property references in WHERE clauses.
        /// </summary>
        /// <param name="navigationPropertyAliases">A dictionary mapping navigation property names to their SQL table aliases</param>
        public void SetNavigationPropertyAliases(Dictionary<string, string> navigationPropertyAliases)
        {
            ArgumentNullException.ThrowIfNull(navigationPropertyAliases);
            _NavigationPropertyAliases = navigationPropertyAliases;
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

                _ => throw new NotSupportedException($"Binary operator '{binary.NodeType}' is not supported in MySQL expressions")
            };

            // Handle null comparisons
            if (right == "NULL")
            {
                if (op == "=") return $"{left} IS NULL";
                if (op == "!=") return $"{left} IS NOT NULL";
            }

            // Handle null coalescing operator (MySQL uses COALESCE function)
            if (binary.NodeType == ExpressionType.Coalesce)
            {
                return $"COALESCE({left}, {right})";
            }

            // Handle string concatenation (MySQL uses CONCAT function)
            if (binary.NodeType == ExpressionType.Add &&
                (binary.Left.Type == typeof(string) || binary.Right.Type == typeof(string)))
            {
                return $"CONCAT({left}, {right})";
            }

            // For logical operators with mixed precedence, add parentheses when needed
            if (op == "AND" || op == "OR")
            {
                // Only add outer parentheses if this is part of a larger expression with different operators
                return $"{left} {op} {right}";
            }

            // For comparison and math operators, no outer parentheses needed
            return $"{left} {op} {right}";
        }

        private string VisitMember(MemberExpression member)
        {
            // Check if this is a property access on the entity parameter (e.g., p.FirstName)
            if (member.Expression is ParameterExpression && member.Member is PropertyInfo propInfo)
            {
                KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                if (mapping.Key != null)
                    return $"`{mapping.Key}`";
            }

            // Special handling for Length property access on method call results (e.g., p.FirstName.Trim().Length)
            if (member.Member.Name == "Length" && member.Expression is MethodCallExpression methodCall &&
                member.Member.DeclaringType == typeof(string))
            {
                // Handle as CHAR_LENGTH(method_result) for MySQL
                string methodResult = Visit(methodCall);
                return $"CHAR_LENGTH({methodResult})";
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
                        return $"CHAR_LENGTH(`{mapping.Key}`)";
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

                // DateTime operations - MySQL specific functions
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

                // String operations
                case "ToUpper":
                case "ToLower":
                case "Trim":
                case "Length":
                case "Substring":
                    return HandleStringFunction(methodCall);

                // Custom Between method (extension method)
                case "Between":
                    return HandleBetween(methodCall);
            }

            // Handle static method calls on specific types
            if (methodCall.Method.DeclaringType != null)
            {
                string typeName = methodCall.Method.DeclaringType.Name;
                if (typeName == "DateTime" && methodCall.Method.Name == "Now")
                {
                    return "NOW()";
                }
                if (typeName == "DateTime" && methodCall.Method.Name == "UtcNow")
                {
                    return "UTC_TIMESTAMP()";
                }
                if (typeName == "DateTime" && methodCall.Method.Name == "Today")
                {
                    return "CURDATE()";
                }
            }

            throw new NotSupportedException($"Method '{methodCall.Method.DeclaringType?.Name}.{methodCall.Method.Name}' is not supported in MySQL expressions. Consider using supported methods like Contains, StartsWith, ToUpper, etc.");
        }

        private string VisitUnary(UnaryExpression unary)
        {
            switch (unary.NodeType)
            {
                case ExpressionType.Not:
                    string operand = Visit(unary.Operand);
                    return $"NOT {operand}";
                case ExpressionType.Negate:
                    string negateOperand = Visit(unary.Operand);
                    return $"-{negateOperand}";
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    // Handle type conversions by visiting the operand
                    return Visit(unary.Operand);
                default:
                    throw new NotSupportedException($"Unary operator '{unary.NodeType}' is not supported in MySQL expressions");
            }
        }

        private string VisitConditional(ConditionalExpression conditional)
        {
            // CASE WHEN expression support
            string test = Visit(conditional.Test);
            string ifTrue = Visit(conditional.IfTrue);
            string ifFalse = Visit(conditional.IfFalse);

            return $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
        }

        private string VisitNewArray(NewArrayExpression newArray)
        {
            List<string> values = new List<string>();
            foreach (Expression expr in newArray.Expressions)
            {
                object? value = GetConstantValue(expr);
                values.Add(FormatValue(value));
            }
            return string.Join(", ", values);
        }

        private string HandleContains(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null && methodCall.Arguments.Count == 1)
            {
                // Check if this is a collection.Contains(item) call
                Type objectType = methodCall.Object.Type;
                if (typeof(System.Collections.IEnumerable).IsAssignableFrom(objectType) && objectType != typeof(string))
                {
                    // Collection.Contains(item) - IN operator
                    System.Collections.IEnumerable? collection = GetConstantValue(methodCall.Object) as System.Collections.IEnumerable;
                    string item = Visit(methodCall.Arguments[0]);

                    if (collection != null)
                    {
                        List<string> values = new List<string>();
                        foreach (object? collectionItem in collection)
                        {
                            values.Add(FormatValue(collectionItem));
                        }
                        return $"{item} IN ({string.Join(", ", values)})";
                    }
                }
                else
                {
                    // String.Contains - LIKE operation
                    string column = Visit(methodCall.Object);
                    object? value = GetConstantValue(methodCall.Arguments[0]);
                    string sanitizedValue = _Sanitizer.SanitizeLikeValue(value?.ToString() ?? "");
                    // Keep the sanitizer's quotes and use CONCAT for safe wildcard addition
                    // Remove outer quotes, add wildcards, then re-quote safely
                    string innerValue = sanitizedValue.Substring(1, sanitizedValue.Length - 2);
                    return $"{column} LIKE CONCAT('%', {_Sanitizer.SanitizeString(innerValue)}, '%')";
                }
            }
            else if (methodCall.Arguments.Count == 2)
            {
                // Static Contains method: collection.Contains(item)
                System.Collections.IEnumerable? collection = GetConstantValue(methodCall.Arguments[0]) as System.Collections.IEnumerable;
                string item = Visit(methodCall.Arguments[1]);

                if (collection != null)
                {
                    List<string> values = new List<string>();
                    foreach (object? collectionItem in collection)
                    {
                        values.Add(FormatValue(collectionItem));
                    }
                    return $"{item} IN ({string.Join(", ", values)})";
                }
            }

            throw new NotSupportedException("Contains method call is not supported in this context. Ensure you're using it with a collection (for IN operations) or string (for LIKE operations).");
        }

        private string HandleBetween(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 3)
            {
                // value.Between(min, max)
                string value = Visit(methodCall.Arguments[0]);
                string min = Visit(methodCall.Arguments[1]);
                string max = Visit(methodCall.Arguments[2]);

                return $"{value} BETWEEN {min} AND {max}";
            }

            throw new NotSupportedException("Between method requires exactly 3 arguments: value.Between(min, max). Example: p.Age.Between(18, 65)");
        }

        private string HandleDateTimeAdd(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null && methodCall.Arguments.Count == 1)
            {
                string dateColumn = Visit(methodCall.Object);
                object? amount = GetConstantValue(methodCall.Arguments[0]);

                string unit = methodCall.Method.Name switch
                {
                    "AddDays" => "DAY",
                    "AddHours" => "HOUR",
                    "AddMinutes" => "MINUTE",
                    "AddSeconds" => "SECOND",
                    "AddMonths" => "MONTH",
                    "AddYears" => "YEAR",
                    _ => throw new NotSupportedException($"DateTime method '{methodCall.Method.Name}' is not supported. Supported methods: AddDays, AddHours, AddMinutes, AddSeconds, AddMonths, AddYears")
                };

                return $"DATE_ADD({dateColumn}, INTERVAL {amount} {unit})";
            }

            throw new NotSupportedException($"DateTime method '{methodCall.Method.Name}' requires a valid DateTime expression as the target object");
        }

        private string HandleDateTimePart(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null)
            {
                string dateColumn = Visit(methodCall.Object);

                string part = methodCall.Method.Name switch
                {
                    "get_Year" => $"YEAR({dateColumn})",
                    "get_Month" => $"MONTH({dateColumn})",
                    "get_Day" => $"DAY({dateColumn})",
                    "get_Hour" => $"HOUR({dateColumn})",
                    "get_Minute" => $"MINUTE({dateColumn})",
                    "get_Second" => $"SECOND({dateColumn})",
                    _ => throw new NotSupportedException($"DateTime property '{methodCall.Method.Name}' is not supported. Supported properties: Year, Month, Day, Hour, Minute, Second")
                };

                return part;
            }

            throw new NotSupportedException($"DateTime property '{methodCall.Method.Name}' requires a valid DateTime expression as the target object");
        }

        private string HandleMathFunction(MethodCallExpression methodCall)
        {
            string functionName = methodCall.Method.Name.ToUpper();

            if (methodCall.Arguments.Count == 1)
            {
                string argument = Visit(methodCall.Arguments[0]);

                return functionName switch
                {
                    "ABS" => $"ABS({argument})",
                    "FLOOR" => $"FLOOR({argument})",
                    "CEILING" => $"CEILING({argument})",
                    "ROUND" => $"ROUND({argument})",
                    "SQRT" => $"SQRT({argument})",
                    "SIN" => $"SIN({argument})",
                    "COS" => $"COS({argument})",
                    "TAN" => $"TAN({argument})",
                    _ => throw new NotSupportedException($"Math function '{functionName}' is not supported. Supported functions: ABS, FLOOR, CEILING, ROUND, SQRT, SIN, COS, TAN")
                };
            }
            else if (methodCall.Arguments.Count == 2 && functionName == "ROUND")
            {
                string value = Visit(methodCall.Arguments[0]);
                string digits = Visit(methodCall.Arguments[1]);
                return $"ROUND({value}, {digits})";
            }

            throw new NotSupportedException($"Math function {functionName} call not supported in this context");
        }

        private string HandleStringFunction(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null)
            {
                string stringColumn = Visit(methodCall.Object);

                return methodCall.Method.Name switch
                {
                    "ToUpper" => $"UPPER({stringColumn})",
                    "ToLower" => $"LOWER({stringColumn})",
                    "Trim" => $"TRIM({stringColumn})",
                    "Length" when methodCall.Method.DeclaringType == typeof(string) => $"CHAR_LENGTH({stringColumn})",
                    "Substring" => HandleSubstring(methodCall, stringColumn),
                    _ => throw new NotSupportedException($"String method {methodCall.Method.Name} is not supported")
                };
            }

            throw new NotSupportedException($"String method {methodCall.Method.Name} call not supported in this context");
        }

        private string HandleSubstring(MethodCallExpression methodCall, string stringColumn)
        {
            if (methodCall.Arguments.Count == 1)
            {
                // Substring(startIndex) - from start index to end
                string startIndex = Visit(methodCall.Arguments[0]);
                // MySQL SUBSTRING is 1-based, C# is 0-based, so we add 1 to the start index
                return $"SUBSTRING({stringColumn}, {startIndex} + 1)";
            }
            else if (methodCall.Arguments.Count == 2)
            {
                // Substring(startIndex, length) - specific length from start index
                string startIndex = Visit(methodCall.Arguments[0]);
                string length = Visit(methodCall.Arguments[1]);
                // MySQL SUBSTRING is 1-based, C# is 0-based, so we add 1 to the start index
                return $"SUBSTRING({stringColumn}, {startIndex} + 1, {length})";
            }
            else
            {
                throw new NotSupportedException($"Substring method with {methodCall.Arguments.Count} arguments is not supported");
            }
        }

        private string ParseUpdateValue(Expression expression)
        {
            switch (expression)
            {
                case ConstantExpression constant:
                    if (constant.Value is DateTime dateTime)
                    {
                        double timeDiff = Math.Abs((DateTime.Now - dateTime).TotalSeconds);
                        if (timeDiff < 5)
                        {
                            return "NOW()";
                        }
                    }
                    return FormatValue(constant.Value);

                case MemberExpression member:
                    if (IsParameterMember(member))
                    {
                        return GetColumnFromExpression(member);
                    }
                    else
                    {
                        object? value = GetMemberValue(member);
                        if (value is DateTime memberDateTime)
                        {
                            double timeDiff = Math.Abs((DateTime.Now - memberDateTime).TotalSeconds);
                            if (timeDiff < 5)
                            {
                                return "NOW()";
                            }
                        }
                        return FormatValue(value);
                    }

                case BinaryExpression binary:
                    return ParseUpdateBinaryExpression(binary);

                case UnaryExpression unary:
                    return ParseUpdateUnaryExpression(unary);

                case ConditionalExpression conditional:
                    return ParseUpdateConditionalExpression(conditional);

                case MethodCallExpression methodCall:
                    return ParseUpdateMethodCall(methodCall);

                case NewExpression newExpr:
                    if (newExpr.Type == typeof(DateTime) && newExpr.Arguments.Count >= 3)
                    {
                        object? year = GetConstantValue(newExpr.Arguments[0]);
                        object? month = GetConstantValue(newExpr.Arguments[1]);
                        object? day = GetConstantValue(newExpr.Arguments[2]);

                        if (newExpr.Arguments.Count >= 6)
                        {
                            object? hour = GetConstantValue(newExpr.Arguments[3]);
                            object? minute = GetConstantValue(newExpr.Arguments[4]);
                            object? second = GetConstantValue(newExpr.Arguments[5]);
                            return FormatValue(new DateTime((int)year!, (int)month!, (int)day!, (int)hour!, (int)minute!, (int)second!));
                        }
                        else
                        {
                            return FormatValue(new DateTime((int)year!, (int)month!, (int)day!));
                        }
                    }
                    goto default;

                default:
                    try
                    {
                        object? value = GetConstantValue(expression);
                        return FormatValue(value);
                    }
                    catch
                    {
                        throw new NotSupportedException($"Update expression type {expression.GetType()} is not supported");
                    }
            }
        }

        private string ParseUpdateBinaryExpression(BinaryExpression binary)
        {
            string left = ParseUpdateValue(binary.Left);
            string right = ParseUpdateValue(binary.Right);

            // Handle string concatenation specifically (MySQL uses CONCAT)
            if (binary.NodeType == ExpressionType.Add &&
                (binary.Left.Type == typeof(string) || binary.Right.Type == typeof(string)))
            {
                return $"CONCAT({left}, {right})";
            }

            string op = binary.NodeType switch
            {
                ExpressionType.Add => "+",
                ExpressionType.Subtract => "-",
                ExpressionType.Multiply => "*",
                ExpressionType.Divide => "/",
                ExpressionType.Modulo => "%",
                ExpressionType.Coalesce => "COALESCE",
                _ => throw new NotSupportedException($"Binary operator {binary.NodeType} is not supported in update expressions")
            };

            if (binary.NodeType == ExpressionType.Coalesce)
            {
                return $"COALESCE({left}, {right})";
            }

            return $"{left} {op} {right}";
        }

        private string ParseUpdateUnaryExpression(UnaryExpression unary)
        {
            switch (unary.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    string negateOperand = ParseUpdateValue(unary.Operand);
                    return $"-{negateOperand}";

                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    return ParseUpdateValue(unary.Operand);

                default:
                    throw new NotSupportedException($"Unary operator {unary.NodeType} is not supported in update expressions");
            }
        }

        private string ParseUpdateConditionalExpression(ConditionalExpression conditional)
        {
            string test = Visit(conditional.Test);
            string ifTrue = ParseUpdateValue(conditional.IfTrue);
            string ifFalse = ParseUpdateValue(conditional.IfFalse);

            return $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
        }

        private string ParseUpdateMethodCall(MethodCallExpression methodCall)
        {
            switch (methodCall.Method.Name)
            {
                case "ToUpper":
                case "ToLower":
                case "Trim":
                    if (methodCall.Object != null)
                    {
                        string column = ParseUpdateValue(methodCall.Object);
                        return methodCall.Method.Name switch
                        {
                            "ToUpper" => $"UPPER({column})",
                            "ToLower" => $"LOWER({column})",
                            "Trim" => $"TRIM({column})",
                            _ => throw new NotSupportedException()
                        };
                    }
                    break;

                case "Substring":
                    if (methodCall.Object != null)
                    {
                        string column = ParseUpdateValue(methodCall.Object);
                        if (methodCall.Arguments.Count == 1)
                        {
                            string start = ParseUpdateValue(methodCall.Arguments[0]);
                            return $"SUBSTRING({column}, {start} + 1)";
                        }
                        else if (methodCall.Arguments.Count == 2)
                        {
                            string start = ParseUpdateValue(methodCall.Arguments[0]);
                            string length = ParseUpdateValue(methodCall.Arguments[1]);
                            return $"SUBSTRING({column}, {start} + 1, {length})";
                        }
                    }
                    break;

                case "Replace":
                    if (methodCall.Object != null && methodCall.Arguments.Count == 2)
                    {
                        string column = ParseUpdateValue(methodCall.Object);
                        string oldValue = ParseUpdateValue(methodCall.Arguments[0]);
                        string newValue = ParseUpdateValue(methodCall.Arguments[1]);
                        return $"REPLACE({column}, {oldValue}, {newValue})";
                    }
                    break;

                case "Concat":
                    if (methodCall.Method.DeclaringType == typeof(string))
                    {
                        List<string> parts = new List<string>();
                        foreach (Expression arg in methodCall.Arguments)
                        {
                            parts.Add(ParseUpdateValue(arg));
                        }
                        return $"CONCAT({string.Join(", ", parts)})";
                    }
                    break;
            }

            if (methodCall.Method.DeclaringType == typeof(DateTime))
            {
                if (methodCall.Method.Name == "Now")
                {
                    return "NOW()";
                }
                else if (methodCall.Method.Name == "UtcNow")
                {
                    return "UTC_TIMESTAMP()";
                }
                else if (methodCall.Method.Name == "Today")
                {
                    return "CURDATE()";
                }
            }

            try
            {
                object? value = GetConstantValue(methodCall);
                return FormatValue(value);
            }
            catch
            {
                throw new NotSupportedException($"Method {methodCall.Method.Name} is not supported in update expressions");
            }
        }

        private bool IsParameterMember(MemberExpression member)
        {
            Expression? current = member;
            while (current != null)
            {
                if (current is ParameterExpression)
                {
                    return true;
                }

                if (current is MemberExpression memberExpr)
                {
                    current = memberExpr.Expression;
                }
                else
                {
                    break;
                }
            }
            return false;
        }

        /// <summary>
        /// Resolves navigation property chains to table alias references.
        /// Supports arbitrary depth navigation (e.g., b.Author.Name, b.Author.Company.Industry, etc.),
        /// self-referencing entities, multiple navigation properties to the same entity type, and circular references.
        /// </summary>
        /// <param name="member">The member expression representing the navigation property chain.</param>
        /// <returns>The fully qualified column reference with table alias, or null if resolution fails.</returns>
        private string? ResolveNavigationPropertyChain(MemberExpression member)
        {
            try
            {
                // Build the property path from the member expression, walking from leaf to root
                List<string> propertyPath = new List<string>();
                Expression current = member;

                // Walk up the member expression chain to build the complete path
                while (current is MemberExpression memberExpr)
                {
                    if (memberExpr.Member is PropertyInfo prop)
                    {
                        propertyPath.Insert(0, prop.Name);
                    }
                    current = memberExpr.Expression!;
                }

                // Verify the root is the entity parameter (e.g., "p" in p.Author.Name)
                if (current is not ParameterExpression)
                {
                    return null;
                }

                // Must have at least 2 elements: navigation property + target property
                // Example: ["Author", "Name"] or ["Author", "Company", "Industry"]
                if (propertyPath.Count < 2)
                {
                    return null;
                }

                // The last element is the target column name
                string targetProperty = propertyPath[propertyPath.Count - 1];

                // Handle special properties that require SQL functions
                if (targetProperty == "Length" && propertyPath.Count >= 2)
                {
                    // For .Length on string properties, wrap in CHAR_LENGTH()
                    // Recursively resolve the navigation path without the Length property
                    List<string> navigationPath = propertyPath.GetRange(0, propertyPath.Count - 1);
                    string? columnRef = ResolveNavigationPath(navigationPath);

                    if (columnRef != null)
                    {
                        return $"CHAR_LENGTH({columnRef})";
                    }
                    return null;
                }

                // Resolve the full navigation path to get the appropriate table alias and column
                return ResolveNavigationPath(propertyPath);
            }
            catch
            {
                // If anything goes wrong, return null to fall back to existing error handling
                return null;
            }
        }

        /// <summary>
        /// Resolves a navigation property path to its fully qualified SQL column reference.
        /// This method handles navigation paths of arbitrary depth by trying progressively
        /// longer path segments until a match is found in the navigation property aliases.
        /// </summary>
        /// <param name="propertyPath">The complete property path (e.g., ["Author", "Company", "Name"]).</param>
        /// <returns>The fully qualified column reference, or null if resolution fails.</returns>
        private string? ResolveNavigationPath(List<string> propertyPath)
        {
            if (propertyPath.Count < 2)
            {
                return null;
            }

            // The last element is always the target column name
            string targetProperty = propertyPath[propertyPath.Count - 1];

            // Try to resolve the navigation path by testing progressively longer path segments
            // This handles nested navigation properties correctly
            //
            // Example path: ["Author", "Company", "Industry"]
            // We try:
            //   1. "Author.Company" -> if found, use its alias for "Industry" column
            //   2. "Author" -> if found, check if "Company" is a column on Author, then fail
            //
            // This approach handles:
            // - Simple navigation: Author.Name -> try "Author", get alias, resolve "Name"
            // - Nested navigation: Author.Company.Industry -> try "Author.Company", get alias, resolve "Industry"
            // - Deep nesting: A.B.C.D.E -> try "A.B.C.D", "A.B.C", "A.B", "A" until match found

            // Start from the longest possible navigation path (excluding the target column)
            // and work backwards to find a match in _NavigationPropertyAliases
            for (int pathLength = propertyPath.Count - 1; pathLength >= 1; pathLength--)
            {
                // Build the navigation path string (e.g., "Author.Company")
                string navigationPath = string.Join(".", propertyPath.GetRange(0, pathLength));

                // Try to find this path in the navigation property aliases
                if (_NavigationPropertyAliases.TryGetValue(navigationPath, out string? tableAlias))
                {
                    // Found a match! Now determine what the target column is

                    // If pathLength equals propertyPath.Count - 1, this is a direct property access
                    // Example: ["Author", "Name"] with pathLength=1 means we found "Author" -> get "Name" column
                    if (pathLength == propertyPath.Count - 1)
                    {
                        string columnName = ConvertPropertyNameToColumnName(targetProperty);
                        return $"{tableAlias}.`{columnName}`";
                    }

                    // If pathLength < propertyPath.Count - 1, there are more navigation properties after this one
                    // This means the remaining path elements represent navigation properties that should have
                    // been included but weren't. This is an error condition.
                    //
                    // Example: ["Author", "Company", "Industry"] with pathLength=1 means we only found "Author"
                    // but "Company" should also be included for this query to work.
                    else
                    {
                        // Build the missing include path for the error message
                        List<string> remainingPath = propertyPath.GetRange(pathLength, propertyPath.Count - pathLength - 1);
                        string missingPath = navigationPath + "." + string.Join(".", remainingPath);

                        throw new InvalidOperationException(
                            $"Navigation property '{missingPath}' is not included in the query. " +
                            $"Found '{navigationPath}' but '{string.Join(".", remainingPath)}' is also required. " +
                            $"Use .Include(x => x.{missingPath.Replace(".", ".")}) to include the complete navigation path. " +
                            $"Available navigation properties: {string.Join(", ", _NavigationPropertyAliases.Keys)}");
                    }
                }
            }

            // No matching navigation path found in aliases
            // This means the first navigation property wasn't included
            string firstNavigation = propertyPath[0];

            throw new InvalidOperationException(
                $"Navigation property '{firstNavigation}' is not included in the query. " +
                $"Use .Include(x => x.{firstNavigation}) to include this navigation property before using it in WHERE clauses. " +
                $"Available navigation properties: {string.Join(", ", _NavigationPropertyAliases.Keys)}");
        }

        /// <summary>
        /// Converts a C# property name to the corresponding database column name
        /// </summary>
        private string ConvertPropertyNameToColumnName(string propertyName)
        {
            // Convert PascalCase to snake_case
            // This should match your database naming convention
            return ConvertToSnakeCase(propertyName);
        }

        /// <summary>
        /// Converts PascalCase strings to snake_case
        /// </summary>
        private string ConvertToSnakeCase(string pascalCase)
        {
            if (string.IsNullOrEmpty(pascalCase))
                return pascalCase;

            // Insert underscores before uppercase letters (except the first character)
            System.Text.StringBuilder result = new System.Text.StringBuilder();

            for (int i = 0; i < pascalCase.Length; i++)
            {
                char c = pascalCase[i];

                // Add underscore before uppercase letters (except first character)
                if (i > 0 && char.IsUpper(c))
                {
                    result.Append('_');
                }

                result.Append(char.ToLowerInvariant(c));
            }

            return result.ToString();
        }

        private object? GetMemberValue(MemberExpression member)
        {
            return GetCachedCompiledExpression(member)();
        }

        private object? GetConstantValue(Expression expression)
        {
            if (expression is ConstantExpression constant)
                return constant.Value;

            return GetCachedCompiledExpression(expression)();
        }

        private string VisitWithPrecedence(Expression expression, ExpressionType parentOperator, bool isLeft)
        {
            // If this is not a binary expression, visit normally without extra parentheses
            if (expression is not BinaryExpression childBinary)
            {
                return Visit(expression);
            }

            // Get operator precedence levels
            int parentPrecedence = GetOperatorPrecedence(parentOperator);
            int childPrecedence = GetOperatorPrecedence(childBinary.NodeType);

            // Add parentheses if:
            // 1. Child has lower precedence than parent
            // 2. Same precedence but right-associative operation on the left side
            bool needsParentheses = childPrecedence < parentPrecedence ||
                                  (childPrecedence == parentPrecedence && isLeft && IsRightAssociative(childBinary.NodeType));

            string result = Visit(expression);
            return needsParentheses ? $"({result})" : result;
        }

        private int GetOperatorPrecedence(ExpressionType operatorType)
        {
            return operatorType switch
            {
                // Highest precedence
                ExpressionType.Multiply or ExpressionType.Divide or ExpressionType.Modulo => 5,
                ExpressionType.Add or ExpressionType.Subtract => 4,

                // Comparison operators
                ExpressionType.Equal or ExpressionType.NotEqual or
                ExpressionType.LessThan or ExpressionType.LessThanOrEqual or
                ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual => 3,

                // Logical AND
                ExpressionType.And or ExpressionType.AndAlso => 2,

                // Logical OR (lowest precedence)
                ExpressionType.Or or ExpressionType.OrElse => 1,

                // Default for unknown operators
                _ => 0
            };
        }

        private bool IsRightAssociative(ExpressionType operatorType)
        {
            // Most operators are left-associative; very few are right-associative in SQL
            // For simplicity, we'll treat all as left-associative
            return false;
        }

        private string FormatValue(object? value)
        {
            // If we're not using parameterized queries, format value directly (backward compatibility)
            if (!_UseParameterizedQueries)
            {
                return _Sanitizer.FormatValue(value!);
            }

            // Generate parameter name and add to collection
            string parameterName = $"@p{_ParameterCounter++}";
            _Parameters.Add((parameterName, value));
            return parameterName;
        }

        private Func<object?> GetCachedCompiledExpression(Expression expression)
        {
            // Try to get existing cached expression using structural equality
            // This enables cache hits for semantically identical expressions even if they are different object instances
            if (_CompiledExpressions.TryGetValue(expression, out Func<object?>? cachedGetter))
            {
                System.Threading.Interlocked.Increment(ref _CacheHits);
                return cachedGetter;
            }

            // Cache miss - compile new expression
            System.Threading.Interlocked.Increment(ref _CacheMisses);

            // Compile the expression to a delegate
            UnaryExpression objectMember = Expression.Convert(expression, typeof(object));
            Expression<Func<object>> getterLambda = Expression.Lambda<Func<object>>(objectMember);
            Func<object?> compiledGetter = getterLambda.Compile();

            // Cache using thread-safe GetOrAdd operation
            // If another thread added the same expression while we were compiling, GetOrAdd will return the existing one
            Func<object?> cachedOrNewGetter = _CompiledExpressions.GetOrAdd(expression, compiledGetter);

            return cachedOrNewGetter;
        }

        private bool ContainsParameterReference(Expression? expression)
        {
            switch (expression)
            {
                case ParameterExpression:
                    return true;
                case MemberExpression member:
                    return ContainsParameterReference(member.Expression);
                case MethodCallExpression methodCall:
                    if (methodCall.Object != null && ContainsParameterReference(methodCall.Object))
                        return true;
                    return methodCall.Arguments.Any(ContainsParameterReference);
                case UnaryExpression unary:
                    return ContainsParameterReference(unary.Operand);
                case BinaryExpression binary:
                    return ContainsParameterReference(binary.Left) || ContainsParameterReference(binary.Right);
                default:
                    return false;
            }
        }

        #endregion
    }

    /// <summary>
    /// Provides structural equality comparison for Expression trees to enable cache hits for semantically identical expressions.
    /// This comparer treats two Expression objects as equal if they have the same structure, node types, and values,
    /// regardless of whether they are the same object instance.
    /// </summary>
    internal class ExpressionStructuralEqualityComparer : IEqualityComparer<Expression>
    {
        #region Public-Members

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        #endregion

        #region Public-Methods

        /// <summary>
        /// Determines whether two Expression objects are structurally equal.
        /// </summary>
        /// <param name="x">The first Expression to compare.</param>
        /// <param name="y">The second Expression to compare.</param>
        /// <returns>True if the expressions have the same structure; otherwise, false.</returns>
        public bool Equals(Expression? x, Expression? y)
        {
            if (ReferenceEquals(x, y))
                return true;

            if (x == null || y == null)
                return false;

            if (x.NodeType != y.NodeType || x.Type != y.Type)
                return false;

            return x.NodeType switch
            {
                ExpressionType.Constant => EqualsConstant((ConstantExpression)x, (ConstantExpression)y),
                ExpressionType.MemberAccess => EqualsMember((MemberExpression)x, (MemberExpression)y),
                ExpressionType.Call => EqualsMethodCall((MethodCallExpression)x, (MethodCallExpression)y),
                ExpressionType.Lambda => EqualsLambda((LambdaExpression)x, (LambdaExpression)y),
                ExpressionType.Parameter => EqualsParameter((ParameterExpression)x, (ParameterExpression)y),
                ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.Not or ExpressionType.Negate or ExpressionType.NegateChecked
                    => EqualsUnary((UnaryExpression)x, (UnaryExpression)y),
                ExpressionType.Add or ExpressionType.AddChecked or ExpressionType.Subtract or ExpressionType.SubtractChecked or
                ExpressionType.Multiply or ExpressionType.MultiplyChecked or ExpressionType.Divide or ExpressionType.Modulo or
                ExpressionType.And or ExpressionType.AndAlso or ExpressionType.Or or ExpressionType.OrElse or
                ExpressionType.Equal or ExpressionType.NotEqual or ExpressionType.LessThan or ExpressionType.LessThanOrEqual or
                ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual or ExpressionType.Coalesce
                    => EqualsBinary((BinaryExpression)x, (BinaryExpression)y),
                ExpressionType.Conditional => EqualsConditional((ConditionalExpression)x, (ConditionalExpression)y),
                ExpressionType.NewArrayInit or ExpressionType.NewArrayBounds => EqualsNewArray((NewArrayExpression)x, (NewArrayExpression)y),
                _ => false // Unknown expression types are not considered equal
            };
        }

        /// <summary>
        /// Returns a hash code for the specified Expression based on its structure.
        /// Structurally equivalent expressions will produce the same hash code.
        /// </summary>
        /// <param name="obj">The Expression for which to get a hash code.</param>
        /// <returns>A hash code for the Expression's structure.</returns>
        public int GetHashCode(Expression obj)
        {
            if (obj == null)
                return 0;

            unchecked
            {
                int hash = (int)obj.NodeType * 397;
                hash = (hash * 397) ^ obj.Type.GetHashCode();

                switch (obj)
                {
                    case ConstantExpression constant:
                        if (constant.Value != null)
                            hash = (hash * 397) ^ constant.Value.GetHashCode();
                        break;

                    case MemberExpression member:
                        hash = (hash * 397) ^ member.Member.GetHashCode();
                        if (member.Expression != null)
                            hash = (hash * 397) ^ GetHashCode(member.Expression);
                        break;

                    case MethodCallExpression methodCall:
                        hash = (hash * 397) ^ methodCall.Method.GetHashCode();
                        if (methodCall.Object != null)
                            hash = (hash * 397) ^ GetHashCode(methodCall.Object);
                        foreach (Expression arg in methodCall.Arguments)
                            hash = (hash * 397) ^ GetHashCode(arg);
                        break;

                    case LambdaExpression lambda:
                        hash = (hash * 397) ^ GetHashCode(lambda.Body);
                        foreach (ParameterExpression param in lambda.Parameters)
                            hash = (hash * 397) ^ GetHashCode(param);
                        break;

                    case ParameterExpression parameter:
                        hash = (hash * 397) ^ parameter.Name!.GetHashCode();
                        break;

                    case UnaryExpression unary:
                        hash = (hash * 397) ^ GetHashCode(unary.Operand);
                        break;

                    case BinaryExpression binary:
                        hash = (hash * 397) ^ GetHashCode(binary.Left);
                        hash = (hash * 397) ^ GetHashCode(binary.Right);
                        break;

                    case ConditionalExpression conditional:
                        hash = (hash * 397) ^ GetHashCode(conditional.Test);
                        hash = (hash * 397) ^ GetHashCode(conditional.IfTrue);
                        hash = (hash * 397) ^ GetHashCode(conditional.IfFalse);
                        break;

                    case NewArrayExpression newArray:
                        foreach (Expression expr in newArray.Expressions)
                            hash = (hash * 397) ^ GetHashCode(expr);
                        break;
                }

                return hash;
            }
        }

        #endregion

        #region Private-Methods

        private bool EqualsConstant(ConstantExpression x, ConstantExpression y)
        {
            if (x.Value == null && y.Value == null)
                return true;

            if (x.Value == null || y.Value == null)
                return false;

            return x.Value.Equals(y.Value);
        }

        private bool EqualsMember(MemberExpression x, MemberExpression y)
        {
            if (x.Member != y.Member)
                return false;

            return Equals(x.Expression, y.Expression);
        }

        private bool EqualsMethodCall(MethodCallExpression x, MethodCallExpression y)
        {
            if (x.Method != y.Method)
                return false;

            if (!Equals(x.Object, y.Object))
                return false;

            if (x.Arguments.Count != y.Arguments.Count)
                return false;

            for (int i = 0; i < x.Arguments.Count; i++)
            {
                if (!Equals(x.Arguments[i], y.Arguments[i]))
                    return false;
            }

            return true;
        }

        private bool EqualsLambda(LambdaExpression x, LambdaExpression y)
        {
            if (x.Parameters.Count != y.Parameters.Count)
                return false;

            for (int i = 0; i < x.Parameters.Count; i++)
            {
                if (!Equals(x.Parameters[i], y.Parameters[i]))
                    return false;
            }

            return Equals(x.Body, y.Body);
        }

        private bool EqualsParameter(ParameterExpression x, ParameterExpression y)
        {
            // Parameters are equal if they have the same name and type
            // This is sufficient for our caching purposes
            return x.Name == y.Name && x.Type == y.Type;
        }

        private bool EqualsUnary(UnaryExpression x, UnaryExpression y)
        {
            if (x.Method != y.Method)
                return false;

            return Equals(x.Operand, y.Operand);
        }

        private bool EqualsBinary(BinaryExpression x, BinaryExpression y)
        {
            if (x.Method != y.Method)
                return false;

            if (!Equals(x.Left, y.Left))
                return false;

            return Equals(x.Right, y.Right);
        }

        private bool EqualsConditional(ConditionalExpression x, ConditionalExpression y)
        {
            if (!Equals(x.Test, y.Test))
                return false;

            if (!Equals(x.IfTrue, y.IfTrue))
                return false;

            return Equals(x.IfFalse, y.IfFalse);
        }

        private bool EqualsNewArray(NewArrayExpression x, NewArrayExpression y)
        {
            if (x.Expressions.Count != y.Expressions.Count)
                return false;

            for (int i = 0; i < x.Expressions.Count; i++)
            {
                if (!Equals(x.Expressions[i], y.Expressions[i]))
                    return false;
            }

            return true;
        }

        #endregion
    }
}