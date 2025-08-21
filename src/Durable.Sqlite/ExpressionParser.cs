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
                    return Visit(unary.Operand);
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
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "!=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.AndAlso => "AND",
                ExpressionType.OrElse => "OR",
                _ => throw new NotSupportedException($"Binary operator {binary.NodeType} is not supported")
            };

            // Handle null comparisons
            if (right == "NULL")
            {
                if (op == "=") return $"{left} IS NULL";
                if (op == "!=") return $"{left} IS NOT NULL";
            }

            return $"({left} {op} {right})";
        }

        private string VisitMember(MemberExpression member)
        {
            if (member.Member is PropertyInfo propInfo)
            {
                var mapping = _ColumnMappings.FirstOrDefault(m => m.Value == propInfo);
                if (mapping.Key != null)
                    return mapping.Key;
            }

            // Handle constant member access
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
                    if (methodCall.Object != null && methodCall.Arguments.Count == 1)
                    {
                        // String.Contains
                        var column = Visit(methodCall.Object);
                        var value = GetConstantValue(methodCall.Arguments[0]);
                        return $"{column} LIKE '%{value}%'";
                    }
                    else if (methodCall.Arguments.Count == 2)
                    {
                        // List.Contains
                        var value = Visit(methodCall.Arguments[1]);
                        var list = GetConstantValue(methodCall.Arguments[0]) as System.Collections.IEnumerable;
                        if (list != null)
                        {
                            var values = new List<string>();
                            foreach (var item in list)
                            {
                                values.Add(FormatValue(item));
                            }
                            return $"{value} IN ({string.Join(", ", values)})";
                        }
                    }
                    break;

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
                _ => value.ToString()
            };
        }
    }
}
