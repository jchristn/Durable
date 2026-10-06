namespace Durable.LiteGraph
{
    using System;
    using System.Collections.Generic;
    using Durable;
    using Durable.Query;

    /// <summary>
    /// Finds the entity types whose rows a query tree reads besides its root: the related entity of each navigation
    /// member and collection predicate and the junction entity of each many-to-many navigation.
    /// Thread safety: stateless; safe for concurrent use.
    /// </summary>
    internal static class LiteGraphRelatedTypes
    {
        #region Public-Methods

        /// <summary>
        /// Adds the related entities a tree references.
        /// </summary>
        /// <param name="node">Tree; null is accepted.</param>
        /// <param name="into">Set the entities are added to, by entity type. Must not be null.</param>
        public static void Collect(QueryNode? node, Dictionary<Type, EntityMetadata> into)
        {
            if (node == null) return;
            Stack<QueryNode> pending = new Stack<QueryNode>();
            pending.Push(node);
            while (pending.Count > 0)
            {
                QueryNode current = pending.Pop();
                switch (current)
                {
                    case ComparisonNode comparison:
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
                        pending.Push(membership.Item);
                        break;
                    case StringMatchNode match:
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
                        foreach (QueryNode argument in function.Arguments) pending.Push(argument);
                        break;
                    case NavigationMemberNode navigation:
                        into[navigation.RelatedSource.Metadata.EntityType] = navigation.RelatedSource.Metadata;
                        pending.Push(navigation.OwnerKey);
                        break;
                    case CollectionNode collection:
                        into[collection.RelatedSource.Metadata.EntityType] = collection.RelatedSource.Metadata;
                        if (collection.Navigation.Kind == NavigationKind.ManyToMany && collection.Navigation.JunctionType != null)
                            into[collection.Navigation.JunctionType] = EntityMetadata.For(collection.Navigation.JunctionType);
                        pending.Push(collection.OwnerKey);
                        if (collection.Predicate != null) pending.Push(collection.Predicate);
                        break;
                    case AggregateNode aggregate:
                        if (aggregate.Operand != null) pending.Push(aggregate.Operand);
                        break;
                }
            }
        }

        #endregion
    }
}
