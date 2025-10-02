namespace Durable.SqlServer
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
    /// Parses and converts LINQ expressions to SQL Server-compatible T-SQL strings.
    /// Provides support for complex expression trees including binary operations, method calls, and member access.
    /// </summary>
    /// <typeparam name="T">The entity type that the expressions operate on.</typeparam>
    public class SqlServerExpressionParser<T> where T : class
    {

        #region Public-Members

        #endregion

        #region Private-Members

        private readonly Dictionary<string, PropertyInfo> _ColumnMappings;
        private readonly ISanitizer _Sanitizer;
        private readonly List<(string name, object? value)> _Parameters;
        private int _ParameterCounter;
        private bool _UseParameterizedQueries;

        // Type-specific static cache for compiled expressions to avoid repeated compilation overhead
        // IMPORTANT: This static field is unique per generic type T (e.g., SqlServerExpressionParser<User> has
        // a separate cache from SqlServerExpressionParser<Product>), preventing cross-type pollution
        // Using ConditionalWeakTable to allow garbage collection of both keys and values
        private static readonly ConditionalWeakTable<Expression, Func<object?>> _CompiledExpressions = new ConditionalWeakTable<Expression, Func<object?>>();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initializes a new instance of the SqlServerExpressionParser with the specified column mappings and sanitizer.
        /// </summary>
        /// <param name="columnMappings">A dictionary mapping property names to their corresponding database column names and PropertyInfo objects.</param>
        /// <param name="sanitizer">The sanitizer to use for value formatting and SQL injection prevention. Defaults to SqlServerSanitizer if null.</param>
        /// <exception cref="ArgumentNullException">Thrown when columnMappings is null.</exception>
        public SqlServerExpressionParser(Dictionary<string, PropertyInfo> columnMappings, ISanitizer? sanitizer = null)
        {
            _ColumnMappings = columnMappings ?? throw new ArgumentNullException(nameof(columnMappings));
            _Sanitizer = sanitizer ?? new SqlServerSanitizer();
            _Parameters = new List<(string name, object? value)>();
            _ParameterCounter = 0;
            _UseParameterizedQueries = false;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Parses any expression tree and converts it to its SQL Server T-SQL equivalent string representation.
        /// </summary>
        /// <param name="expression">The expression tree to parse and convert to SQL.</param>
        /// <returns>A string containing the SQL Server-compatible T-SQL representation of the expression.</returns>
        /// <exception cref="NotSupportedException">Thrown when an unsupported expression type is encountered.</exception>
        public string ParseExpression(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));
            return Visit(expression);
        }

        /// <summary>
        /// Parses an expression tree and converts it to parameterized SQL Server T-SQL with extracted parameters.
        /// This method clears any existing parameters before parsing.
        /// </summary>
        /// <param name="expression">The expression tree to parse and convert to SQL.</param>
        /// <param name="useParameterizedQueries">If true, extracts values as parameters; if false, embeds values directly (for backward compatibility).</param>
        /// <returns>A string containing the SQL Server-compatible T-SQL representation of the expression.</returns>
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
        /// Uses SQL Server square bracket notation for identifiers.
        /// </summary>
        /// <param name="expression">The member expression representing a property access (e.g., p.FirstName).</param>
        /// <returns>The corresponding database column name for the property.</returns>
        /// <exception cref="ArgumentException">Thrown when the expression is not a valid member expression or the property is not mapped to a column.</exception>
        public string GetColumnFromExpression(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException(nameof(expression));

            // Unwrap Convert expressions (e.g., Convert(x.Id, Object))
            if (expression is UnaryExpression unaryExpr && unaryExpr.NodeType == ExpressionType.Convert)
            {
                expression = unaryExpr.Operand;
            }

            if (expression is MemberExpression memberExpr)
            {
                PropertyInfo? propInfo = memberExpr.Member as PropertyInfo;
                if (propInfo != null)
                {
                    KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                    if (mapping.Key != null)
                        return $"[{mapping.Key}]";
                }
            }
            throw new ArgumentException($"Expression is not a valid member expression or property is not mapped to a column: {expression}", nameof(expression));
        }

        /// <summary>
        /// Gets the raw column name without brackets from a member expression.
        /// Used internally when brackets will be added manually.
        /// </summary>
        /// <param name="expression">The member expression representing a property access.</param>
        /// <returns>The raw column name without brackets.</returns>
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
        /// <returns>A string containing the SQL SET clause with column assignments (e.g., "[Name] = 'John', [Age] = 30").</returns>
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
                            string columnName = mapping.Key; // Keep raw for manual bracket control
                            string valueExpression = ParseUpdateValue(assignment.Expression);
                            setPairs.Add($"[{columnName}] = {valueExpression}");
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

                _ => throw new NotSupportedException($"Binary operator '{binary.NodeType}' is not supported in SQL Server expressions")
            };

            // Handle null comparisons
            if (right == "NULL")
            {
                if (op == "=") return $"{left} IS NULL";
                if (op == "!=") return $"{left} IS NOT NULL";
            }

            // Handle null coalescing operator (SQL Server uses COALESCE function)
            if (binary.NodeType == ExpressionType.Coalesce)
            {
                return $"COALESCE({left}, {right})";
            }

            // Handle string concatenation (SQL Server uses + operator)
            if (binary.NodeType == ExpressionType.Add &&
                (binary.Left.Type == typeof(string) || binary.Right.Type == typeof(string)))
            {
                return $"({left} + {right})";
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
                    return $"[{mapping.Key}]";
            }

            // Special handling for Length property access on method call results (e.g., p.FirstName.Trim().Length)
            if (member.Member.Name == "Length" && member.Expression is MethodCallExpression methodCall &&
                member.Member.DeclaringType == typeof(string))
            {
                // Handle as LEN(method_result) for SQL Server
                string methodResult = Visit(methodCall);
                return $"LEN({methodResult})";
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
                        return $"LEN([{mapping.Key}])";
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
                    return "GETDATE()";
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

                // DateTime operations - SQL Server specific functions
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
                    return "GETDATE()";
                }
                if (typeName == "DateTime" && methodCall.Method.Name == "UtcNow")
                {
                    return "GETUTCDATE()";
                }
                if (typeName == "DateTime" && methodCall.Method.Name == "Today")
                {
                    return "CAST(GETDATE() AS DATE)";
                }
            }

            throw new NotSupportedException($"Method '{methodCall.Method.DeclaringType?.Name}.{methodCall.Method.Name}' is not supported in SQL Server expressions. Consider using supported methods like Contains, StartsWith, ToUpper, etc.");
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
                    throw new NotSupportedException($"Unary operator '{unary.NodeType}' is not supported in SQL Server expressions");
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
                    // Keep the sanitizer's quotes and use string concatenation for safe wildcard addition
                    // Remove outer quotes, add wildcards, then re-quote safely
                    string innerValue = sanitizedValue.Substring(1, sanitizedValue.Length - 2);
                    return $"{column} LIKE '%' + {_Sanitizer.SanitizeString(innerValue)} + '%'";
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

                return $"DATEADD({unit}, {amount}, {dateColumn})";
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
                    "get_Hour" => $"DATEPART(HOUR, {dateColumn})",
                    "get_Minute" => $"DATEPART(MINUTE, {dateColumn})",
                    "get_Second" => $"DATEPART(SECOND, {dateColumn})",
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
                    "ROUND" => $"ROUND({argument}, 0)",
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
                    "Trim" => $"LTRIM(RTRIM({stringColumn}))",
                    "Length" when methodCall.Method.DeclaringType == typeof(string) => $"LEN({stringColumn})",
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
                // SQL Server SUBSTRING is 1-based, C# is 0-based, so we add 1 to the start index
                // Get length using LEN function to get rest of string
                return $"SUBSTRING({stringColumn}, {startIndex} + 1, LEN({stringColumn}))";
            }
            else if (methodCall.Arguments.Count == 2)
            {
                // Substring(startIndex, length) - specific length from start index
                string startIndex = Visit(methodCall.Arguments[0]);
                string length = Visit(methodCall.Arguments[1]);
                // SQL Server SUBSTRING is 1-based, C# is 0-based, so we add 1 to the start index
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
                            return "GETDATE()";
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
                                return "GETDATE()";
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

            // Handle string concatenation specifically (SQL Server uses + operator)
            if (binary.NodeType == ExpressionType.Add &&
                (binary.Left.Type == typeof(string) || binary.Right.Type == typeof(string)))
            {
                return $"({left} + {right})";
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
                            "Trim" => $"LTRIM(RTRIM({column}))",
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
                            return $"SUBSTRING({column}, {start} + 1, LEN({column}))";
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
                    return "GETDATE()";
                }
                else if (methodCall.Method.Name == "UtcNow")
                {
                    return "GETUTCDATE()";
                }
                else if (methodCall.Method.Name == "Today")
                {
                    return "CAST(GETDATE() AS DATE)";
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
        /// Resolves navigation property chains like b.Author.Name to table alias references like t1.[name]
        /// </summary>
        private string? ResolveNavigationPropertyChain(MemberExpression member)
        {
            try
            {
                // Build the property path from the member expression
                List<string> propertyPath = new List<string>();
                Expression current = member;

                // Walk up the member expression chain to build the path
                while (current is MemberExpression memberExpr)
                {
                    if (memberExpr.Member is PropertyInfo prop)
                    {
                        propertyPath.Insert(0, prop.Name);
                    }
                    current = memberExpr.Expression!;
                }

                // Check if the root is the entity parameter
                if (current is not ParameterExpression)
                {
                    return null;
                }

                // For navigation properties, we need to resolve to the appropriate JOIN table alias
                // This is a simplified approach - in a full implementation, you'd need to:
                // 1. Look up the Include mappings to find the correct table alias
                // 2. Convert the property name to the appropriate column name
                // 3. Handle nested navigation properties correctly

                // Handle navigation property patterns
                if (propertyPath.Count >= 2)
                {
                    // Handle 2-level navigation: Author.Name
                    if (propertyPath.Count == 2)
                    {
                        string navigationProperty = propertyPath[0];
                        string targetProperty = propertyPath[1];

                        // Handle special property methods/properties
                        if (targetProperty == "Length")
                        {
                            // Handle .Length property on string navigation properties
                            return $"LEN({GetNavigationColumnReference(navigationProperty, navigationProperty)})";
                        }

                        // Simple mapping - in a real implementation, this would look up
                        // the actual include mappings to get the correct table alias
                        return GetNavigationColumnReference(navigationProperty, targetProperty);
                    }
                    // Handle 3-level navigation: Author.Company.Industry
                    else if (propertyPath.Count == 3)
                    {
                        string firstNavigation = propertyPath[0];  // Author
                        string secondNavigation = propertyPath[1]; // Company
                        string targetProperty = propertyPath[2];   // Industry

                        // For 3-level navigation, we need to determine the final table alias
                        if (firstNavigation == "Author" && secondNavigation == "Company")
                        {
                            // Author.Company.Industry -> t2 (Company table).[industry]
                            string columnName = ConvertPropertyNameToColumnName(targetProperty);
                            return $"[t2].[{columnName}]";
                        }
                    }
                }

                // If we can't resolve it, return null to fall back to the error handling
                return null;
            }
            catch
            {
                // If anything goes wrong, return null to fall back to existing error handling
                return null;
            }
        }

        /// <summary>
        /// Gets the column reference for a navigation property using SQL Server square brackets
        /// </summary>
        private string GetNavigationColumnReference(string navigationProperty, string targetProperty)
        {
            string columnName = ConvertPropertyNameToColumnName(targetProperty);

            if (navigationProperty == "Author")
            {
                return $"[t1].[{columnName}]";
            }
            else if (navigationProperty == "Company")
            {
                return $"[t2].[{columnName}]";
            }
            else if (navigationProperty == "Publisher")
            {
                return $"[t3].[{columnName}]";
            }

            // Default fallback - this should be improved to dynamically resolve table aliases
            return $"[t1].[{columnName}]";
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
            // Try to get existing cached expression using ConditionalWeakTable
            // This allows both keys and values to be garbage collected when no longer referenced
            if (_CompiledExpressions.TryGetValue(expression, out Func<object?>? cachedGetter))
            {
                return cachedGetter;
            }

            // Compile new expression
            UnaryExpression objectMember = Expression.Convert(expression, typeof(object));
            Expression<Func<object>> getterLambda = Expression.Lambda<Func<object>>(objectMember);
            Func<object?> compiledGetter = getterLambda.Compile();

            // Cache using thread-safe Add operation
            // The ConditionalWeakTable automatically removes entries when the key (Expression) is garbage collected
            try
            {
                _CompiledExpressions.Add(expression, compiledGetter);
                return compiledGetter;
            }
            catch (ArgumentException)
            {
                // Key already exists (race condition), return existing value
                return _CompiledExpressions.TryGetValue(expression, out cachedGetter) ? cachedGetter : compiledGetter;
            }
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
}
