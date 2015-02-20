package edu.caltech.nanodb.qeval;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.PredicateUtils;
import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.HashedGroupAggregateNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SortNode;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<PlanNode>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        PlanNode plan = null;
        boolean handledProjectEarly = false;

        // Create a subplan that generates the relation specified by the FROM
        // clause.  If there are joins in the FROM clause then this will be a
        // tree of plan-nodes.
        FromClause fromClause = selClause.getFromClause();
        if (fromClause == null) {
            // The query doesn't have a FROM clause, so we need to generate
            // just a single row.  We will do that here, just in case the
            // user is silly and decides to do a WHERE or an ORDER BY or other
            // silly thing.

            List<SelectValue> selectValues = selClause.getSelectValues();

            // Do what little error checking we can...
            for (SelectValue sv : selectValues) {
                if (sv.isWildcard()) {
                    throw new IllegalArgumentException(
                        "Wildcard expressions are not allowed without " +
                            "a FROM clause");
                }
                else if (sv.isSimpleColumnValue()) {
                    throw new IllegalArgumentException("Column references " +
                        "make no sense without a FROM clause");
                }
            }

            plan = new ProjectNode(selectValues);
            handledProjectEarly = true;
        }

        // Look for aggregate function calls, and transform expressions that
        // include them so that we can compute them all in one grouping /
        // aggregate plan node.
        AggregateFunctionExtractor extractor = prepareAggregates(selClause);

        if (fromClause != null) {
            // Pull out the top-level conjuncts from the WHERE clause on the
            // query, since we will handle them in special ways if we have
            // outer joins.

            HashSet<Expression> whereConjuncts = new HashSet<Expression>();
            PredicateUtils.collectConjuncts(selClause.getWhereExpr(), whereConjuncts);

            // Create an optimal join plan from the top-level from-clause and the
            // top-level conjuncts.
            JoinComponent joinComp = makeJoinPlan(fromClause, whereConjuncts);
            plan = joinComp.joinPlan;

            HashSet<Expression> unusedConjuncts =
                new HashSet<Expression>(whereConjuncts);
            unusedConjuncts.removeAll(joinComp.conjunctsUsed);

            Expression finalPredicate = PredicateUtils.makePredicate(unusedConjuncts);
            if (finalPredicate != null)
                plan = addPredicateToPlan(plan, finalPredicate);
        }

        // Handle grouping and aggregation next, if there are any grouping
        // specifications or aggregate operations.
        List<Expression> groupByExprs = selClause.getGroupByExprs();
        if (!groupByExprs.isEmpty() || extractor.foundAggregates()) {
            // Get the aggregates, if present.
            Map<String, FunctionCall> aggregates = extractor.getAggregateCalls();

            // By default, use a hash-based grouping/aggregate node.  Later
            // we can replace with a sort-based grouping/aggregate node if
            // it would be more efficient.
            plan = new HashedGroupAggregateNode(plan, groupByExprs, aggregates);

            // Apply the HAVING predicate, if one is present.
            Expression havingExpr = selClause.getHavingExpr();
            if (havingExpr != null) {
                plan = addPredicateToPlan(plan, havingExpr);
            }
        }

        // Depending on the SELECT clause, create a project node at the top of
        // the tree.
        if (!selClause.isTrivialProject())
            plan = new ProjectNode(plan, selClause.getSelectValues());

        // Finally, apply any sorting at the end.
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (!orderByExprs.isEmpty())
            plan = new SortNode(plan, orderByExprs);

        plan.prepare();

        return plan;
    }


    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<Expression>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<FromClause>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:  " +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     *
     * TODO:  FILL IN DETAILS.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        if (fromClause.getClauseType() == FromClause.ClauseType.JOIN_EXPR &&
            !fromClause.isOuterJoin()) {
            // This is an inner-join expression.  Pull out the conjuncts if
            // there are any, and then collect details from both children of
            // the join.

            FromClause.JoinConditionType condType = fromClause.getConditionType();
            if (condType != null) {
                PredicateUtils.collectConjuncts(fromClause.getPreparedJoinExpr(),
                    conjuncts);
            }

            collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
            collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
        }
        else {
            // This is either a base table, a derived table (a SELECT subquery),
            // or an outer join.  Add it to the list of leaf FROM-clauses.
            leafFromClauses.add(fromClause);
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<JoinComponent>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<Expression>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * Depending on the clause's {@link FromClause#getClauseType type},
     * the plan tree will comprise varying operations, such as:
     * <ul>
     *   <li>
     *     {@link edu.caltech.nanodb.commands.FromClause.ClauseType#BASE_TABLE} -
     *     the clause is a simple table reference, so a simple select operation
     *     is constructed via {@link #makeSimpleSelect}.
     *   </li>
     *   <li>
     *     {@link edu.caltech.nanodb.commands.FromClause.ClauseType#SELECT_SUBQUERY} -
     *     the clause is a <tt>SELECT</tt> subquery, so a plan subtree is
     *     constructed by a recursive call to {@link #makePlan}.
     *   </li>
     *   <li>
     *     {@link edu.caltech.nanodb.commands.FromClause.ClauseType#JOIN_EXPR}
     *     <b>(outer joins only!)</b> - the clause is an outer join of two
     *     relations.  Because outer joins are so constrained in what conjuncts
     *     can be pushed down through them, we treat them as leaf-components in
     *     this optimizer as well.  The child-plans of the outer join are
     *     constructed by recursively invoking the {@link #makeJoinPlan} method,
     *     and then an outer join is constructed from the two children.
     *   </li>
     * </ul>
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts applied
     *        in this plan from the <tt>conjuncts</tt> collection should be added
     *        to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        PlanNode plan;

        FromClause.ClauseType clauseType = fromClause.getClauseType();
        switch (clauseType) {
        case BASE_TABLE:
        case SELECT_SUBQUERY:

            if (clauseType == FromClause.ClauseType.SELECT_SUBQUERY) {
                // This clause is a SQL subquery, so generate a plan from the
                // subquery and return it.
                plan = makePlan(fromClause.getSelectClause(), null);
            }
            else {
                // This clause is a base-table, so we just generate a file-scan
                // plan node for the table.
                plan = makeSimpleSelect(fromClause.getTableName(), null, null);
            }

            // If the FROM-clause renames the result, apply the renaming here.
            if (fromClause.isRenamed())
                plan = new RenameNode(plan, fromClause.getResultName());

            plan.prepare();
            Schema schema = plan.getSchema();

            // If possible, construct a predicate for this leaf node by adding
            // conjuncts that are specific to only this leaf plan-node.
            //
            // Do not remove those conjuncts from the set of unused conjuncts.

            PredicateUtils.findExprsUsingSchemas(conjuncts, false,
                leafConjuncts, schema);

            Expression leafPredicate = PredicateUtils.makePredicate(leafConjuncts);
            if (leafPredicate != null) {
                plan = addPredicateToPlan(plan, leafPredicate);
            }

            break;

        case JOIN_EXPR:
            if (!fromClause.isOuterJoin()) {
                throw new IllegalArgumentException(
                    "This method only supports outer joins.  Got " +
                    fromClause);
            }

            Collection<Expression> childConjuncts;

            childConjuncts = conjuncts;
            if (fromClause.hasOuterJoinOnRight())
                childConjuncts = null;
            JoinComponent leftComp =
                makeJoinPlan(fromClause.getLeftChild(), childConjuncts);

            childConjuncts = conjuncts;
            if (fromClause.hasOuterJoinOnLeft())
                childConjuncts = null;
            JoinComponent rightComp =
                makeJoinPlan(fromClause.getRightChild(), childConjuncts);

            plan = new NestedLoopsJoinNode(leftComp.joinPlan, rightComp.joinPlan,
                fromClause.getJoinType(), fromClause.getPreparedJoinExpr());

            leafConjuncts.addAll(leftComp.conjunctsUsed);
            leafConjuncts.addAll(rightComp.conjunctsUsed);

            break;

        default:
            throw new IllegalArgumentException(
                "Unrecognized from-clause type:  " + fromClause.getClauseType());
        }

        plan.prepare();

        return plan;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans =
            new HashMap<HashSet<PlanNode>, JoinComponent>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<HashSet<PlanNode>, JoinComponent>();

            // Iterate over each plan in the current set.  Those plans already
            // join n leaf-plans together.  We will generate more plans that
            // join n+1 leaves together.
            for (JoinComponent prevComponent : joinPlans.values()) {
                HashSet<PlanNode> prevLeavesUsed = prevComponent.leavesUsed;
                PlanNode prevPlan = prevComponent.joinPlan;
                HashSet<Expression> prevConjunctsUsed = prevComponent.conjunctsUsed;
                Schema prevSchema = prevPlan.getSchema();

                // Iterate over the leaf plans; try to add each leaf-plan to
                // this join-plan, to produce new plans that join n+1 leaves.
                for (JoinComponent leaf : leafComponents) {
                    PlanNode leafPlan = leaf.joinPlan;

                    // If the leaf-plan already appears in this join, skip it!
                    if (prevLeavesUsed.contains(leafPlan))
                        continue;

                    // The new plan we generate will involve everything from the
                    // old plan, plus the leaf-plan we are joining in.
                    // Of course, we could join in different orders, so consider
                    // both join-orderings.

                    HashSet<PlanNode> newLeavesUsed =
                        new HashSet<PlanNode>(prevLeavesUsed);
                    newLeavesUsed.add(leafPlan);

                    // Compute the join predicate between these two subplans.
                    // (We presume that the subplans have both been prepared.)

                    Schema leafSchema = leafPlan.getSchema();

                    // Find the conjuncts that reference both subplans' schemas.
                    // Also remove those predicates from the original set of all
                    // conjuncts.

                    // These are the conjuncts already used by the subplans.
                    HashSet<Expression> subplanConjuncts =
                        new HashSet<Expression>(prevConjunctsUsed);
                    subplanConjuncts.addAll(leaf.conjunctsUsed);

                    // These are the conjuncts still unused for this join pair.
                    HashSet<Expression> unusedConjuncts =
                        new HashSet<Expression>(conjuncts);
                    unusedConjuncts.removeAll(subplanConjuncts);

                    // These are the conjuncts relevant for the join pair.
                    HashSet<Expression> joinConjuncts = new HashSet<Expression>();
                    PredicateUtils.findExprsUsingSchemas(unusedConjuncts, true,
                        joinConjuncts, leafSchema, prevSchema);

                    Expression joinPredicate =
                        PredicateUtils.makePredicate(joinConjuncts);

                    // Join the leaf-plan with the previous optimal plan, and
                    // see if it's better than whatever we currently have.
                    // We only build LEFT-DEEP join plans; the leaf node goes
                    // on the right!

                    NestedLoopsJoinNode newJoinPlan =
                        new NestedLoopsJoinNode(prevPlan, leafPlan,
                        JoinType.INNER, joinPredicate);
                    newJoinPlan.prepare();
                    PlanCost newJoinCost = newJoinPlan.getCost();

                    joinConjuncts.addAll(subplanConjuncts);
                    JoinComponent joinComponent = new JoinComponent(newJoinPlan,
                        newLeavesUsed, joinConjuncts);

                    JoinComponent currentBest = nextJoinPlans.get(newLeavesUsed);
                    if (currentBest == null) {
                        logger.info("Setting current best-plan.");
                        nextJoinPlans.put(newLeavesUsed, joinComponent);
                    }
                    else {
                        PlanCost bestCost = currentBest.joinPlan.getCost();
                        if (newJoinCost.cpuCost < bestCost.cpuCost) {
                            logger.info("Replacing current best-plan with new plan!");
                            nextJoinPlans.put(newLeavesUsed, joinComponent);
                        }
                    }
                }
            }

            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
        List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}
