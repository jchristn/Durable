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
        private readonly Dictionary<string, PropertyInfo> _ColumnMappings;

        public ExpressionParser(Dictionary<string, PropertyInfo> columnMappings)
        {
            _ColumnMappings = columnMappings;
        }

        public string ParseExpression(Expression expression)
        {
            return Visit(expression);
        }

        public string GetColumnFromExpression(Expression expression)
        {
            if (expression is MemberExpression memberExpr)
            {
                var propInfo = memberExpr.Member as PropertyInfo;
                if (propInfo != null)
                {
                    var mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
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
                var setPairs = new List<string>();
                foreach (var binding in memberInit.Bindings)
                {
                    if (binding is MemberAssignment assignment)
                    {
                        var propInfo = assignment.Member as PropertyInfo;
                        var mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                        if (mapping.Key != null)
                        {
                            var value = GetConstantValue(assignment.Expression);
                            setPairs.Add($"{mapping.Key} = {FormatValue(value)}");
                        }
                    }
                }
                return string.Join(", ", setPairs);
            }
            throw new ArgumentException("Update expression must be a member initialization expression");
        }

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
            var left = Visit(binary.Left);
            var right = Visit(binary.Right);

            var op = binary.NodeType switch
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
                var mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                if (mapping.Key != null)
                    return mapping.Key;
            }

            // Special handling for Length property access on method call results (e.g., p.FirstName.Trim().Length)
            if (member.Member.Name == "Length" && member.Expression is MethodCallExpression methodCall && 
                member.Member.DeclaringType == typeof(string))
            {
                // Handle as LENGTH(method_result)
                var methodResult = Visit(methodCall);
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
            var value = GetMemberValue(member);
            return FormatValue(value);
        }

        private string VisitConstant(ConstantExpression constant)
        {
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
                        var left = Visit(methodCall.Object);
                        var right = Visit(methodCall.Arguments[0]);
                        return $"({left} = {right})";
                    }
                    else if (methodCall.Arguments.Count == 2)
                    {
                        // Static Equals method
                        var left = Visit(methodCall.Arguments[0]);
                        var right = Visit(methodCall.Arguments[1]);
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
                        var column = Visit(methodCall.Object);
                        var value = GetConstantValue(methodCall.Arguments[0]);
                        return $"{column} LIKE '{value}%'";
                    }
                    break;

                case "EndsWith":
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        var column = Visit(methodCall.Object);
                        var value = GetConstantValue(methodCall.Arguments[0]);
                        return $"{column} LIKE '%{value}'";
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
                var typeName = methodCall.Method.DeclaringType.Name;
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
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

        private object GetConstantValue(Expression expression)
        {
            if (expression is ConstantExpression constant)
                return constant.Value;

            var objectMember = Expression.Convert(expression, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

        private string FormatValue(object value)
        {
            return value switch
            {
                null => "NULL",
                string s => $"'{s.Replace("'", "''")}'",
                bool b => b ? "1" : "0",
                DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
                DateTimeOffset dto => $"'{dto:yyyy-MM-dd HH:mm:ss}'",
                TimeSpan ts => $"'{ts}'",
                _ => value.ToString()
            };
        }

        private string VisitUnary(UnaryExpression unary)
        {
            switch (unary.NodeType)
            {
                case ExpressionType.Not:
                    var operand = Visit(unary.Operand);
                    return $"NOT ({operand})";
                case ExpressionType.Negate:
                    var negateOperand = Visit(unary.Operand);
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
            var test = Visit(conditional.Test);
            var ifTrue = Visit(conditional.IfTrue);
            var ifFalse = Visit(conditional.IfFalse);
            
            return $"CASE WHEN {test} THEN {ifTrue} ELSE {ifFalse} END";
        }

        private string VisitNewArray(NewArrayExpression newArray)
        {
            var values = new List<string>();
            foreach (var expr in newArray.Expressions)
            {
                var value = GetConstantValue(expr);
                values.Add(FormatValue(value));
            }
            return string.Join(", ", values);
        }

        private string HandleContains(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null && methodCall.Arguments.Count == 1)
            {
                // Check if this is a collection.Contains(item) call
                var objectType = methodCall.Object.Type;
                if (typeof(System.Collections.IEnumerable).IsAssignableFrom(objectType) && objectType != typeof(string))
                {
                    // Collection.Contains(item) - IN operator
                    var collection = GetConstantValue(methodCall.Object) as System.Collections.IEnumerable;
                    var item = Visit(methodCall.Arguments[0]);
                    
                    if (collection != null)
                    {
                        var values = new List<string>();
                        foreach (var collectionItem in collection)
                        {
                            values.Add(FormatValue(collectionItem));
                        }
                        return $"{item} IN ({string.Join(", ", values)})";
                    }
                }
                else
                {
                    // String.Contains - LIKE operation
                    var column = Visit(methodCall.Object);
                    var value = GetConstantValue(methodCall.Arguments[0]);
                    return $"{column} LIKE '%{value}%'";
                }
            }
            else if (methodCall.Arguments.Count == 2)
            {
                // Static Contains method: collection.Contains(item)
                var collection = GetConstantValue(methodCall.Arguments[0]) as System.Collections.IEnumerable;
                var item = Visit(methodCall.Arguments[1]);
                
                if (collection != null)
                {
                    var values = new List<string>();
                    foreach (var collectionItem in collection)
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
                var collection = GetConstantValue(methodCall.Arguments[0]) as System.Collections.IEnumerable;
                if (collection != null)
                {
                    var hasItems = collection.Cast<object>().Any();
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
                var value = Visit(methodCall.Arguments[0]);
                var min = Visit(methodCall.Arguments[1]);
                var max = Visit(methodCall.Arguments[2]);
                
                return $"({value} BETWEEN {min} AND {max})";
            }
            
            throw new NotSupportedException("Between method requires exactly 3 arguments: value, min, max");
        }

        private string HandleDateTimeAdd(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null && methodCall.Arguments.Count == 1)
            {
                var dateColumn = Visit(methodCall.Object);
                var amount = GetConstantValue(methodCall.Arguments[0]);
                
                var modifier = methodCall.Method.Name switch
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
                var dateColumn = Visit(methodCall.Object);
                
                var part = methodCall.Method.Name switch
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
            var functionName = methodCall.Method.Name.ToUpper();
            
            if (methodCall.Arguments.Count == 1)
            {
                var argument = Visit(methodCall.Arguments[0]);
                
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
                var value = Visit(methodCall.Arguments[0]);
                var digits = Visit(methodCall.Arguments[1]);
                return $"ROUND({value}, {digits})";
            }
            
            throw new NotSupportedException($"Math function {functionName} call not supported in this context");
        }

        private string HandleStringFunction(MethodCallExpression methodCall)
        {
            if (methodCall.Object != null)
            {
                var stringColumn = Visit(methodCall.Object);
                
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
                var value = Visit(methodCall.Arguments[0]);
                var values = new List<string>();
                
                // Handle params array or IEnumerable
                if (methodCall.Arguments.Count == 2 && methodCall.Arguments[1].Type.IsAssignableFrom(typeof(System.Collections.IEnumerable)))
                {
                    var collection = GetConstantValue(methodCall.Arguments[1]) as System.Collections.IEnumerable;
                    if (collection != null)
                    {
                        foreach (var item in collection)
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
                        var item = GetConstantValue(methodCall.Arguments[i]);
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
                var value = Visit(methodCall.Arguments[0]);
                var values = new List<string>();
                
                // Handle params array or IEnumerable
                if (methodCall.Arguments.Count == 2 && methodCall.Arguments[1].Type.IsAssignableFrom(typeof(System.Collections.IEnumerable)))
                {
                    var collection = GetConstantValue(methodCall.Arguments[1]) as System.Collections.IEnumerable;
                    if (collection != null)
                    {
                        foreach (var item in collection)
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
                        var item = GetConstantValue(methodCall.Arguments[i]);
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
                var value = Visit(methodCall.Arguments[0]);
                return $"{value} IS NULL";
            }
            
            throw new NotSupportedException("IsNull method requires exactly 1 argument");
        }

        private string HandleIsNotNull(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                var value = Visit(methodCall.Arguments[0]);
                return $"{value} IS NOT NULL";
            }
            
            throw new NotSupportedException("IsNotNull method requires exactly 1 argument");
        }

        private string HandleIsNullOrEmpty(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                var value = Visit(methodCall.Arguments[0]);
                return $"({value} IS NULL OR {value} = '')";
            }
            
            throw new NotSupportedException("IsNullOrEmpty method requires exactly 1 argument");
        }

        private string HandleIsNotNullOrEmpty(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                var value = Visit(methodCall.Arguments[0]);
                return $"({value} IS NOT NULL AND {value} != '')";
            }
            
            throw new NotSupportedException("IsNotNullOrEmpty method requires exactly 1 argument");
        }

        private string HandleIsNullOrWhiteSpace(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                var value = Visit(methodCall.Arguments[0]);
                return $"({value} IS NULL OR TRIM({value}) = '')";
            }
            
            throw new NotSupportedException("IsNullOrWhiteSpace method requires exactly 1 argument");
        }

        private string HandleIsNotNullOrWhiteSpace(MethodCallExpression methodCall)
        {
            if (methodCall.Arguments.Count == 1)
            {
                var value = Visit(methodCall.Arguments[0]);
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
    }
}
