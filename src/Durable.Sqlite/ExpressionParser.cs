namespace Durable.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;
    public class ExpressionParser<T> where T : class
    {
        #region Public-Members
        
        #endregion

        #region Private-Members
        
        private readonly Dictionary<string, PropertyInfo> _ColumnMappings;
        private readonly ISanitizer _Sanitizer;
        
        #endregion

        #region Constructors-and-Factories
        
        public ExpressionParser(Dictionary<string, PropertyInfo> columnMappings, ISanitizer sanitizer = null)
        {
            _ColumnMappings = columnMappings;
            _Sanitizer = sanitizer ?? new SqliteSanitizer();
        }
        
        #endregion

        #region Public-Methods
        
        public string ParseExpression(Expression expression)
        {
            return Visit(expression);
        }

        public string GetColumnFromExpression(Expression expression)
        {
            if (expression is MemberExpression memberExpr)
            {
                PropertyInfo propInfo = memberExpr.Member as PropertyInfo;
                if (propInfo != null)
                {
                    KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                    if (mapping.Key != null)
                        return mapping.Key;
                }
            }
            throw new ArgumentException($"Cannot get column from expression: {expression}");
        }

        public string ParseUpdateExpression(Expression<Func<T, T>> updateExpression)
        {
            if (updateExpression.Body is MemberInitExpression memberInit)
            {
                List<string> setPairs = new List<string>();
                foreach (MemberBinding binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                    {
                        PropertyInfo propInfo = assignment.Member as PropertyInfo;
                        KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                        if (mapping.Key != null)
                        {
                            string columnName = mapping.Key;
                            string valueExpression = ParseUpdateValue(assignment.Expression);
                            setPairs.Add($"{columnName} = {valueExpression}");
                        }
                    }
                }
                return string.Join(", ", setPairs);
            }
            throw new ArgumentException("Update expression must be a member initialization expression");
        }

        public List<(string ColumnName, string Alias, PropertyInfo SourceProperty, PropertyInfo TargetProperty)> ParseSelectExpression<TResult>(Expression<Func<T, TResult>> selector)
        {
            List<(string ColumnName, string Alias, PropertyInfo SourceProperty, PropertyInfo TargetProperty)> mappings = new List<(string, string, PropertyInfo, PropertyInfo)>();

            switch (selector.Body)
            {
                case NewExpression newExpr:
                    // Handle anonymous types and constructor initialization
                    for (int i = 0; i < newExpr.Arguments.Count; i++)
                    {
                        Expression arg = newExpr.Arguments[i];
                        PropertyInfo targetProp = null;
                        
                        if (newExpr.Members != null && i < newExpr.Members.Count)
                        {
                            targetProp = newExpr.Members[i] as PropertyInfo;
                        }

                        if (arg is MemberExpression memberExpr)
                        {
                            PropertyInfo sourceProp = memberExpr.Member as PropertyInfo;
                            if (sourceProp != null)
                            {
                                KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == sourceProp);
                                if (mapping.Key != null)
                                {
                                    string alias = targetProp?.Name ?? sourceProp.Name;
                                    mappings.Add((mapping.Key, alias, sourceProp, targetProp));
                                }
                            }
                        }
                    }
                    break;

                case MemberInitExpression memberInit:
                    // Handle member initialization expressions
                    foreach (MemberBinding binding in memberInit.Bindings)
                    {
                        if (binding is MemberAssignment assignment)
                        {
                            PropertyInfo targetProp = assignment.Member as PropertyInfo;
                            
                            if (assignment.Expression is MemberExpression memberExpr)
                            {
                                PropertyInfo sourceProp = memberExpr.Member as PropertyInfo;
                                if (sourceProp != null)
                                {
                                    KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == sourceProp);
                                    if (mapping.Key != null)
                                    {
                                        mappings.Add((mapping.Key, targetProp.Name, sourceProp, targetProp));
                                    }
                                }
                            }
                        }
                    }
                    break;

                case MemberExpression memberExpr:
                    // Handle single member selection
                    PropertyInfo prop = memberExpr.Member as PropertyInfo;
                    if (prop != null)
                    {
                        KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == prop);
                        if (mapping.Key != null)
                        {
                            mappings.Add((mapping.Key, prop.Name, prop, prop));
                        }
                    }
                    break;

                case ParameterExpression:
                    // Select all columns (identity projection)
                    foreach (KeyValuePair<string, PropertyInfo> kvp in _ColumnMappings)
                    {
                        mappings.Add((kvp.Key, kvp.Key, kvp.Value, kvp.Value));
                    }
                    break;
            }

            return mappings;
        }

        private string ParseUpdateValue(Expression expression)
        {
            switch (expression)
            {
                case ConstantExpression constant:
                    if (constant.Value is DateTime dateTime)
                    {
                        var timeDiff = Math.Abs((DateTime.Now - dateTime).TotalSeconds);
                        if (timeDiff < 5)
                        {
                            return "datetime('now')";
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
                        object value = GetMemberValue(member);
                        if (value is DateTime memberDateTime)
                        {
                            var timeDiff = Math.Abs((DateTime.Now - memberDateTime).TotalSeconds);
                            if (timeDiff < 5)
                            {
                                return "datetime('now')";
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
                        object year = GetConstantValue(newExpr.Arguments[0]);
                        object month = GetConstantValue(newExpr.Arguments[1]);
                        object day = GetConstantValue(newExpr.Arguments[2]);
                        
                        if (newExpr.Arguments.Count >= 6)
                        {
                            object hour = GetConstantValue(newExpr.Arguments[3]);
                            object minute = GetConstantValue(newExpr.Arguments[4]);
                            object second = GetConstantValue(newExpr.Arguments[5]);
                            return FormatValue(new DateTime((int)year, (int)month, (int)day, (int)hour, (int)minute, (int)second));
                        }
                        else
                        {
                            return FormatValue(new DateTime((int)year, (int)month, (int)day));
                        }
                    }
                    goto default;
                    
                default:
                    try
                    {
                        object value = GetConstantValue(expression);
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
            
            // Handle string concatenation specifically
            if (binary.NodeType == ExpressionType.Add && 
                (binary.Left.Type == typeof(string) || binary.Right.Type == typeof(string)))
            {
                return $"({left} || {right})";
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
            
            return $"({left} {op} {right})";
        }

        private string ParseUpdateUnaryExpression(UnaryExpression unary)
        {
            switch (unary.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    string negateOperand = ParseUpdateValue(unary.Operand);
                    return $"-({negateOperand})";
                    
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
                            return $"SUBSTR({column}, {start} + 1)";
                        }
                        else if (methodCall.Arguments.Count == 2)
                        {
                            string start = ParseUpdateValue(methodCall.Arguments[0]);
                            string length = ParseUpdateValue(methodCall.Arguments[1]);
                            return $"SUBSTR({column}, {start} + 1, {length})";
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
                        return string.Join(" || ", parts);
                    }
                    break;
                    
                case "Round":
                case "Floor":
                case "Ceiling":
                case "Abs":
                    if (methodCall.Method.DeclaringType == typeof(Math))
                    {
                        string functionName = methodCall.Method.Name.ToUpper();
                        if (methodCall.Arguments.Count == 1)
                        {
                            string argument = ParseUpdateValue(methodCall.Arguments[0]);
                            return $"{functionName}({argument})";
                        }
                        else if (methodCall.Arguments.Count == 2 && functionName == "ROUND")
                        {
                            string value = ParseUpdateValue(methodCall.Arguments[0]);
                            string digits = ParseUpdateValue(methodCall.Arguments[1]);
                            return $"ROUND({value}, {digits})";
                        }
                    }
                    break;
                    
                case "Max":
                case "Min":
                    if (methodCall.Method.DeclaringType == typeof(Math))
                    {
                        if (methodCall.Arguments.Count == 2)
                        {
                            string arg1 = ParseUpdateValue(methodCall.Arguments[0]);
                            string arg2 = ParseUpdateValue(methodCall.Arguments[1]);
                            return $"{methodCall.Method.Name.ToUpper()}({arg1}, {arg2})";
                        }
                    }
                    break;
                    
                case "AddDays":
                case "AddHours":
                case "AddMinutes":
                case "AddSeconds":
                case "AddMonths":
                case "AddYears":
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        string dateColumn = ParseUpdateValue(methodCall.Object);
                        string amount = ParseUpdateValue(methodCall.Arguments[0]);
                        
                        string modifier = methodCall.Method.Name switch
                        {
                            "AddDays" => "days",
                            "AddHours" => "hours",
                            "AddMinutes" => "minutes",
                            "AddSeconds" => "seconds",
                            "AddMonths" => "months",
                            "AddYears" => "years",
                            _ => throw new NotSupportedException()
                        };
                        
                        return $"datetime({dateColumn}, {amount} || ' {modifier}')";
                    }
                    break;
            }
            
            if (methodCall.Method.DeclaringType == typeof(DateTime))
            {
                if (methodCall.Method.Name == "Now")
                {
                    return "datetime('now')";
                }
                else if (methodCall.Method.Name == "UtcNow")
                {
                    return "datetime('now', 'utc')";
                }
                else if (methodCall.Method.Name == "Today")
                {
                    return "date('now')";
                }
            }
            
            try
            {
                object value = GetConstantValue(methodCall);
                return FormatValue(value);
            }
            catch
            {
                throw new NotSupportedException($"Method {methodCall.Method.Name} is not supported in update expressions");
            }
        }

        private bool IsParameterMember(MemberExpression member)
        {
            Expression current = member;
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
                    throw new NotSupportedException($"Expression type {expression.GetType()} is not supported");
            }
        }

        private string VisitBinary(BinaryExpression binary)
        {
            string left = Visit(binary.Left);
            string right = Visit(binary.Right);

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
                ExpressionType.Power => "POWER",
                
                _ => throw new NotSupportedException($"Binary operator {binary.NodeType} is not supported")
            };

            // Handle null comparisons
            if (right == "NULL")
            {
                if (op == "=") return $"{left} IS NULL";
                if (op == "!=") return $"{left} IS NOT NULL";
            }

            // Special handling for POWER operator
            if (binary.NodeType == ExpressionType.Power)
            {
                return $"POWER({left}, {right})";
            }

            // For logical operators, ensure proper parentheses for complex expressions
            if (op == "AND" || op == "OR")
            {
                return $"({left} {op} {right})";
            }

            return $"({left} {op} {right})";
        }

        private string VisitMember(MemberExpression member)
        {
            // Check if this is a property access on the entity parameter (e.g., p.FirstName)
            if (member.Expression is ParameterExpression && member.Member is PropertyInfo propInfo)
            {
                KeyValuePair<string, PropertyInfo> mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                if (mapping.Key != null)
                    return mapping.Key;
            }

            // Special handling for Length property access on method call results (e.g., p.FirstName.Trim().Length)
            if (member.Member.Name == "Length" && member.Expression is MethodCallExpression methodCall && 
                member.Member.DeclaringType == typeof(string))
            {
                // Handle as LENGTH(method_result)
                string methodResult = Visit(methodCall);
                return $"LENGTH({methodResult})";
            }

            // Check if this member expression ultimately references the entity parameter through method calls
            if (ContainsParameterReference(member))
            {
                // This is likely a property access result that should be treated as SQL expression
                // For other cases not handled above, we might need to extend this logic
                throw new NotSupportedException($"Complex member access '{member}' is not supported yet. Please handle this pattern in the method call processing.");
            }

            // Handle constant member access or property chains
            object value = GetMemberValue(member);
            return FormatValue(value);
        }

        private string VisitConstant(ConstantExpression constant)
        {
            if (constant.Value is DateTime dateTime)
            {
                var timeDiff = Math.Abs((DateTime.Now - dateTime).TotalSeconds);
                if (timeDiff < 5)
                {
                    return "datetime('now')";
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
                        return $"({left} = {right})";
                    }
                    else if (methodCall.Arguments.Count == 2)
                    {
                        // Static Equals method
                        string left = Visit(methodCall.Arguments[0]);
                        string right = Visit(methodCall.Arguments[1]);
                        return $"({left} = {right})";
                    }
                    break;

                case "Contains":
                    return HandleContains(methodCall);

                case "Any":
                    return HandleAny(methodCall);

                case "StartsWith":
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        string column = Visit(methodCall.Object);
                        object value = GetConstantValue(methodCall.Arguments[0]);
                        string sanitizedValue = _Sanitizer.SanitizeLikeValue(value?.ToString());
                        // Remove quotes and add % at the end
                        string innerValue = sanitizedValue.Trim('\'');
                        return $"{column} LIKE '{innerValue}%'";
                    }
                    break;

                case "EndsWith":
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        string column = Visit(methodCall.Object);
                        object value = GetConstantValue(methodCall.Arguments[0]);
                        string sanitizedValue = _Sanitizer.SanitizeLikeValue(value?.ToString());
                        // Remove quotes and add % at the beginning
                        string innerValue = sanitizedValue.Trim('\'');
                        return $"{column} LIKE '%{innerValue}'";
                    }
                    break;

                // DateTime operations
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
                    return HandleStringFunction(methodCall);

                // Custom Between method (extension method)
                case "Between":
                    return HandleBetween(methodCall);

                // Extension methods for additional functionality
                case "In":
                    return HandleIn(methodCall);

                case "NotIn":
                    return HandleNotIn(methodCall);

                case "IsNull":
                    return HandleIsNull(methodCall);

                case "IsNotNull":
                    return HandleIsNotNull(methodCall);

                case "IsNullOrEmpty":
                    return HandleIsNullOrEmpty(methodCall);

                case "IsNotNullOrEmpty":
                    return HandleIsNotNullOrEmpty(methodCall);

                case "IsNullOrWhiteSpace":
                    return HandleIsNullOrWhiteSpace(methodCall);

                case "IsNotNullOrWhiteSpace":
                    return HandleIsNotNullOrWhiteSpace(methodCall);
            }

            // Handle static method calls on specific types
            if (methodCall.Method.DeclaringType != null)
            {
                string typeName = methodCall.Method.DeclaringType.Name;
                if (typeName == "DateTime" && methodCall.Method.Name == "Now")
                {
                    return $"datetime('now')";
                }
                if (typeName == "DateTime" && methodCall.Method.Name == "UtcNow")
                {
                    return $"datetime('now', 'utc')";
                }
                if (typeName == "DateTime" && methodCall.Method.Name == "Today")
                {
                    return $"date('now')";
                }
            }

            throw new NotSupportedException($"Method {methodCall.Method.Name} is not supported");
        }

        private object GetMemberValue(MemberExpression member)
        {
            UnaryExpression objectMember = Expression.Convert(member, typeof(object));
            Expression<Func<object>> getterLambda = Expression.Lambda<Func<object>>(objectMember);
            Func<object> getter = getterLambda.Compile();
            return getter();
        }

        private object GetConstantValue(Expression expression)
        {
            if (expression is ConstantExpression constant)
                return constant.Value;

            UnaryExpression objectMember = Expression.Convert(expression, typeof(object));
            Expression<Func<object>> getterLambda = Expression.Lambda<Func<object>>(objectMember);
            Func<object> getter = getterLambda.Compile();
            return getter();
        }

        private string FormatValue(object value)
        {
            return _Sanitizer.FormatValue(value);
        }

        private string VisitUnary(UnaryExpression unary)
        {
            switch (unary.NodeType)
            {
                case ExpressionType.Not:
                    string operand = Visit(unary.Operand);
                    return $"NOT ({operand})";
                case ExpressionType.Negate:
                    string negateOperand = Visit(unary.Operand);
                    return $"-({negateOperand})";
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    // Handle type conversions by visiting the operand
                    return Visit(unary.Operand);
                default:
                    throw new NotSupportedException($"Unary operator {unary.NodeType} is not supported");
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
                object value = GetConstantValue(expr);
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
                    System.Collections.IEnumerable collection = GetConstantValue(methodCall.Object) as System.Collections.IEnumerable;
                    string item = Visit(methodCall.Arguments[0]);
                    
                    if (collection != null)
                    {
                        List<string> values = new List<string>();
                        foreach (object collectionItem in collection)
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
                    object value = GetConstantValue(methodCall.Arguments[0]);
                    string sanitizedValue = _Sanitizer.SanitizeLikeValue(value?.ToString());
                    // Remove quotes and add % around the value
                    string innerValue = sanitizedValue.Trim('\'');
                    return $"{column} LIKE '%{innerValue}%'";
                }
            }
            else if (methodCall.Arguments.Count == 2)
            {
                // Static Contains method: collection.Contains(item)
                System.Collections.IEnumerable collection = GetConstantValue(methodCall.Arguments[0]) as System.Collections.IEnumerable;
                string item = Visit(methodCall.Arguments[1]);
                
                if (collection != null)
                {
                    List<string> values = new List<string>();
                    foreach (object collectionItem in collection)
                    {
                        values.Add(FormatValue(collectionItem));
                    }
                    return $"{item} IN ({string.Join(", ", values)})";
                }
            }
            
            throw new NotSupportedException("Contains method call not supported in this context");
        }

        private string HandleAny(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                // collection.Any() - check if collection has any items
                System.Collections.IEnumerable collection = GetConstantValue(methodCall.Arguments[0]) as System.Collections.IEnumerable;
                if (collection != null)
                {
                    bool hasItems = collection.Cast<object>().Any();
                    return hasItems ? "1" : "0";
                }
            }
            
            throw new NotSupportedException("Any method call not supported in this context");
        }

        private string HandleBetween(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 3)
            {
                // value.Between(min, max)
                string value = Visit(methodCall.Arguments[0]);
                string min = Visit(methodCall.Arguments[1]);
                string max = Visit(methodCall.Arguments[2]);
                
                return $"({value} BETWEEN {min} AND {max})";
            }
            
            throw new NotSupportedException("Between method requires exactly 3 arguments: value, min, max");
        }

        private string HandleDateTimeAdd(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null && methodCall.Arguments.Count == 1)
            {
                string dateColumn = Visit(methodCall.Object);
                object amount = GetConstantValue(methodCall.Arguments[0]);
                
                string modifier = methodCall.Method.Name switch
                {
                    "AddDays" => $"{amount} days",
                    "AddHours" => $"{amount} hours",
                    "AddMinutes" => $"{amount} minutes",
                    "AddSeconds" => $"{amount} seconds",
                    "AddMonths" => $"{amount} months",
                    "AddYears" => $"{amount} years",
                    _ => throw new NotSupportedException($"DateTime method {methodCall.Method.Name} is not supported")
                };
                
                return $"datetime({dateColumn}, '{modifier}')";
            }
            
            throw new NotSupportedException($"DateTime {methodCall.Method.Name} method call not supported in this context");
        }

        private string HandleDateTimePart(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null)
            {
                string dateColumn = Visit(methodCall.Object);
                
                string part = methodCall.Method.Name switch
                {
                    "get_Year" => "strftime('%Y', " + dateColumn + ")",
                    "get_Month" => "strftime('%m', " + dateColumn + ")",
                    "get_Day" => "strftime('%d', " + dateColumn + ")",
                    "get_Hour" => "strftime('%H', " + dateColumn + ")",
                    "get_Minute" => "strftime('%M', " + dateColumn + ")",
                    "get_Second" => "strftime('%S', " + dateColumn + ")",
                    _ => throw new NotSupportedException($"DateTime property {methodCall.Method.Name} is not supported")
                };
                
                return part;
            }
            
            throw new NotSupportedException($"DateTime property {methodCall.Method.Name} call not supported in this context");
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
                    _ => throw new NotSupportedException($"Math function {functionName} is not supported")
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
                    "Length" when methodCall.Method.DeclaringType == typeof(string) => $"LENGTH({stringColumn})",
                    _ => throw new NotSupportedException($"String method {methodCall.Method.Name} is not supported")
                };
            }
            
            throw new NotSupportedException($"String method {methodCall.Method.Name} call not supported in this context");
        }

        private string HandleIn(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count >= 2)
            {
                string value = Visit(methodCall.Arguments[0]);
                List<string> values = new List<string>();
                
                // Handle params array or IEnumerable
                if (methodCall.Arguments.Count == 2 && methodCall.Arguments[1].Type.IsAssignableFrom(typeof(System.Collections.IEnumerable)))
                {
                    System.Collections.IEnumerable collection = GetConstantValue(methodCall.Arguments[1]) as System.Collections.IEnumerable;
                    if (collection != null)
                    {
                        foreach (object item in collection)
                        {
                            values.Add(FormatValue(item));
                        }
                    }
                }
                else
                {
                    // Handle params array
                    for (int i = 1; i < methodCall.Arguments.Count; i++)
                    {
                        object item = GetConstantValue(methodCall.Arguments[i]);
                        values.Add(FormatValue(item));
                    }
                }
                
                return $"{value} IN ({string.Join(", ", values)})";
            }
            
            throw new NotSupportedException("In method requires at least 2 arguments: value and collection/values");
        }

        private string HandleNotIn(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count >= 2)
            {
                string value = Visit(methodCall.Arguments[0]);
                List<string> values = new List<string>();
                
                // Handle params array or IEnumerable
                if (methodCall.Arguments.Count == 2 && methodCall.Arguments[1].Type.IsAssignableFrom(typeof(System.Collections.IEnumerable)))
                {
                    System.Collections.IEnumerable collection = GetConstantValue(methodCall.Arguments[1]) as System.Collections.IEnumerable;
                    if (collection != null)
                    {
                        foreach (object item in collection)
                        {
                            values.Add(FormatValue(item));
                        }
                    }
                }
                else
                {
                    // Handle params array
                    for (int i = 1; i < methodCall.Arguments.Count; i++)
                    {
                        object item = GetConstantValue(methodCall.Arguments[i]);
                        values.Add(FormatValue(item));
                    }
                }
                
                return $"{value} NOT IN ({string.Join(", ", values)})";
            }
            
            throw new NotSupportedException("NotIn method requires at least 2 arguments: value and collection/values");
        }

        private string HandleIsNull(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                string value = Visit(methodCall.Arguments[0]);
                return $"{value} IS NULL";
            }
            
            throw new NotSupportedException("IsNull method requires exactly 1 argument");
        }

        private string HandleIsNotNull(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                string value = Visit(methodCall.Arguments[0]);
                return $"{value} IS NOT NULL";
            }
            
            throw new NotSupportedException("IsNotNull method requires exactly 1 argument");
        }

        private string HandleIsNullOrEmpty(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                string value = Visit(methodCall.Arguments[0]);
                return $"({value} IS NULL OR {value} = '')";
            }
            
            throw new NotSupportedException("IsNullOrEmpty method requires exactly 1 argument");
        }

        private string HandleIsNotNullOrEmpty(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                string value = Visit(methodCall.Arguments[0]);
                return $"({value} IS NOT NULL AND {value} != '')";
            }
            
            throw new NotSupportedException("IsNotNullOrEmpty method requires exactly 1 argument");
        }

        private string HandleIsNullOrWhiteSpace(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                string value = Visit(methodCall.Arguments[0]);
                return $"({value} IS NULL OR TRIM({value}) = '')";
            }
            
            throw new NotSupportedException("IsNullOrWhiteSpace method requires exactly 1 argument");
        }

        private string HandleIsNotNullOrWhiteSpace(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                string value = Visit(methodCall.Arguments[0]);
                return $"({value} IS NOT NULL AND TRIM({value}) != '')";
            }
            
            throw new NotSupportedException("IsNotNullOrWhiteSpace method requires exactly 1 argument");
        }

        private bool ContainsParameterReference(Expression expression)
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
