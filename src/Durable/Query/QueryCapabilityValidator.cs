namespace Durable.Query
{
    using System;
    using System.Collections.Generic;
    using Durable;

    /// <summary>
    /// Checks a query tree against a backend's <see cref="RepositoryCapabilities"/> and throws a
    /// <see cref="NotSupportedException"/> naming the missing capability before the backend sees the query.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    public static class QueryCapabilityValidator
    {
        #region Public-Methods

        /// <summary>
        /// Throws when a capability is missing.
        /// </summary>
        /// <param name="available">Capabilities the backend supports.</param>
        /// <param name="required">Capability the operation needs.</param>
        /// <param name="operation">Operation name for the message. Must not be null.</param>
        /// <exception cref="NotSupportedException">Thrown when <paramref name="available"/> lacks <paramref name="required"/>.</exception>
        public static void Require(RepositoryCapabilities available, RepositoryCapabilities required, string operation)
        {
            ArgumentNullException.ThrowIfNull(operation);
            if ((available & required) != required)
                throw new NotSupportedException(operation + " requires the " + (required & ~available) + " capability, which this repository backend does not support.");
        }

        /// <summary>
        /// Throws when a node tree uses a feature the backend does not support.
        /// </summary>
        /// <param name="node">Tree to check; null is accepted.</param>
        /// <param name="available">Capabilities the backend supports.</param>
        /// <exception cref="NotSupportedException">Thrown for the first unsupported feature found.</exception>
        public static void Validate(QueryNode? node, RepositoryCapabilities available)
        {
            if (node == null) return;
            Stack<QueryNode> pending = new Stack<QueryNode>();
            pending.Push(node);
            while (pending.Count > 0)
            {
                QueryNode current = pending.Pop();
                switch (current)
                {
                    case ColumnNode:
                    case ValueNode:
                        break;
                    case ComparisonNode comparison:
                        RequireStringMode(available, comparison.StringMode, comparison.Left.ClrType == typeof(string) || comparison.Right.ClrType == typeof(string));
                        pending.Push(comparison.Left);
                        pending.Push(comparison.Right);
                        break;
                    case LogicalNode logical:
                        pending.Push(logical.Left);
                        pending.Push(logical.Right);
                        break;
                    case NotNode not:
                        pending.Push(not.Operand);
                        break;
                    case NullCheckNode nullCheck:
                        pending.Push(nullCheck.Operand);
                        break;
                    case InNode membership:
                        RequireStringMode(available, membership.StringMode, membership.Item.ClrType == typeof(string));
                        pending.Push(membership.Item);
                        break;
                    case StringMatchNode match:
                        RequireStringMode(available, match.Mode, true);
                        pending.Push(match.Target);
                        pending.Push(match.Pattern);
                        break;
                    case StringTestNode test:
                        pending.Push(test.Operand);
                        break;
                    case ArithmeticNode arithmetic:
                        pending.Push(arithmetic.Left);
                        pending.Push(arithmetic.Right);
                        break;
                    case NegateNode negate:
                        pending.Push(negate.Operand);
                        break;
                    case ConcatNode concat:
                        foreach (QueryNode part in concat.Parts) pending.Push(part);
                        break;
                    case CoalesceNode coalesce:
                        pending.Push(coalesce.Left);
                        pending.Push(coalesce.Right);
                        break;
                    case ConditionalNode conditional:
                        pending.Push(conditional.Test);
                        pending.Push(conditional.IfTrue);
                        pending.Push(conditional.IfFalse);
                        break;
                    case FunctionNode function:
                        Require(available, RepositoryCapabilities.Functions, "Function " + function.Function);
                        RequireStringMode(available, function.StringMode, function.Function == QueryFunction.Replace || function.Function == QueryFunction.IndexOf);
                        foreach (QueryNode argument in function.Arguments) pending.Push(argument);
                        break;
                    case NavigationMemberNode navigation:
                        Require(available, RepositoryCapabilities.NavigationPredicates, "Navigation member '" + navigation.Navigation.Name + "." + navigation.Column.Property.Name + "'");
                        pending.Push(navigation.OwnerKey);
                        break;
                    case CollectionNode collection:
                        Require(available, RepositoryCapabilities.NavigationPredicates, "Collection navigation " + collection.Operation + " on '" + collection.Navigation.Name + "'");
                        if (collection.Navigation.Kind == NavigationKind.ManyToMany)
                            Require(available, RepositoryCapabilities.ManyToMany, "Many-to-many navigation '" + collection.Navigation.Name + "'");
                        pending.Push(collection.OwnerKey);
                        if (collection.Predicate != null) pending.Push(collection.Predicate);
                        break;
                    case AggregateNode aggregate:
                        Require(available, RepositoryCapabilities.Grouping, "Group aggregate " + aggregate.Function);
                        if (aggregate.Operand != null) pending.Push(aggregate.Operand);
                        break;
                    default:
                        throw new NotSupportedException("Unknown query node " + current.GetType().Name + ".");
                }
            }
        }

        #endregion

        #region Private-Methods

        private static void RequireStringMode(RepositoryCapabilities available, StringMatchMode mode, bool applies)
        {
            if (applies && mode != StringMatchMode.Database)
                Require(available, RepositoryCapabilities.StringMatchModes, "String matching mode " + mode);
        }

        #endregion
    }
}
