package edu.caltech.nanodb.qeval;


import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * Created by donnie on 10/31/14.
 */
public abstract class AbstractPlannerImpl implements Planner {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(AbstractPlannerImpl.class);


    protected StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    protected AggregateFunctionExtractor prepareAggregates(SelectClause selClause) {
        // Analyze all expressions in the SELECT, WHERE and HAVING clauses for
        // aggregate function calls.  (Obviously, if the WHERE clause contains
        // aggregates then it's an error!)

        List<SelectValue> selectValues = selClause.getSelectValues();
        AggregateFunctionExtractor extractor = new AggregateFunctionExtractor();

        // Make sure the WHERE clause doesn't contain any aggregates...
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            whereExpr.traverse(extractor);
            if (extractor.foundAggregates()) {
                // TODO:  Maybe list the aggregates found?
                throw new IllegalArgumentException(
                    "WHERE clause cannot contain aggregates.");
            }
        }

        // Make sure no conditions in the FROM clause contain aggregates...
        if (selClause.getFromClause() != null)
            checkJoinsForAggregates(selClause.getFromClause(), extractor);

        // Now it's OK to find aggregates, so scan SELECT and HAVING clauses.

        for (SelectValue sv : selectValues) {
            // Skip select-values that aren't expressions!
            if (!sv.isExpression())
                continue;

            Expression e = sv.getExpression().traverse(extractor);
            sv.setExpression(e);
        }

        Expression havingExpr = selClause.getHavingExpr();
        if (havingExpr != null) {
            havingExpr = havingExpr.traverse(extractor);
            selClause.setHavingExpr(havingExpr);
        }

        if (extractor.foundAggregates()) {
            // Print out some useful details about what happened during the
            // aggregate-function extraction.

            Map<String, FunctionCall> aggregates = extractor.getAggregateCalls();
            logger.info(String.format("Found %d aggregate functions:",
                aggregates.size()));
            for (String name : aggregates.keySet()) {
                FunctionCall agg = aggregates.get(name);
                logger.info(" * " + name + " = " + agg);
            }

            logger.info("Transformed select-values:");
            for (SelectValue sv : selectValues)
                logger.info(" * " + sv.getExpression());

            logger.info("Transformed HAVING clause:  " + havingExpr);
        }

        return extractor;
    }


    /**
     * This helper function traverses the FROM clauses in the query and
     * ensures that no FROM predicate contains an aggregate function.
     *
     * @param fromClause the FROM clause to check
     * @param extractor the extractor used to identify and transform aggregate
     *        functions
     *
     * @throws IllegalArgumentException if an aggregate function is found
     *         with a FROM clause's predicate.
     */
    protected void checkJoinsForAggregates(FromClause fromClause,
                                         AggregateFunctionExtractor extractor) {
        if (fromClause.getClauseType() == FromClause.ClauseType.JOIN_EXPR) {
            Expression joinExpr = fromClause.getPreparedJoinExpr();
            if (joinExpr != null) {
                joinExpr.traverse(extractor);
                if (extractor.foundAggregates()) {
                    // TODO:  Maybe list the aggregates found?
                    throw new IllegalArgumentException(
                        "Predicates in the FROM clause cannot contain aggregates.");
                }
            }

            checkJoinsForAggregates(fromClause.getLeftChild(), extractor);
            checkJoinsForAggregates(fromClause.getRightChild(), extractor);
        }
    }


    /**
     * This helper function takes a query plan and a selection predicate, and
     * adds the predicate to the plan in a reasonably intelligent way.
     * <p>
     * If the plan is a subclass of the {@link SelectNode} then the select
     * node's predicate is updated to include the predicate.  Specifically, if
     * the select node already has a predicate then one of the following occurs:
     * <ul>
     *   <li>If the select node currently has no predicate, the new predicate is
     *       assigned to the select node.</li>
     *   <li>If the select node has a predicate whose top node is a
     *       {@link BooleanOperator} of type <tt>AND</tt>, this predicate is
     *       added as a new term on that node.</li>
     *   <li>If the select node has some other kind of non-<tt>null</tt>
     *       predicate then this method creates a new top-level <tt>AND</tt>
     *       operation that will combine the two predicates into one.</li>
     * </ul>
     * <p>
     * If the plan is <em>not</em> a subclass of the {@link SelectNode} then a
     * new {@link SimpleFilterNode} is added above the current plan node, with
     * the specified predicate.
     *
     * @param plan the plan to add the selection predicate to
     *
     * @param predicate the selection predicate to add to the plan
     *
     * @return the (possibly new) top plan-node for the plan with the selection
     *         predicate applied
     */
    protected PlanNode addPredicateToPlan(PlanNode plan, Expression predicate) {
        if (plan instanceof SelectNode) {
            SelectNode selectNode = (SelectNode) plan;

            if (selectNode.predicate != null) {
                // There is already an existing predicate.  Add this as a
                // conjunct to the existing predicate.
                Expression fsPred = selectNode.predicate;
                boolean handled = false;

                // If the current predicate is an AND operation, just make
                // the where-expression an additional term.
                if (fsPred instanceof BooleanOperator) {
                    BooleanOperator bool = (BooleanOperator) fsPred;
                    if (bool.getType() == BooleanOperator.Type.AND_EXPR) {
                        bool.addTerm(predicate);
                        handled = true;
                    }
                }

                if (!handled) {
                    // Oops, the current file-scan predicate wasn't an AND.
                    // Create an AND expression instead.
                    BooleanOperator bool =
                        new BooleanOperator(BooleanOperator.Type.AND_EXPR);
                    bool.addTerm(fsPred);
                    bool.addTerm(predicate);
                    selectNode.predicate = bool;
                }
            }
            else {
                // Simple - just add where-expression onto the file-scan.
                selectNode.predicate = predicate;
            }
        }
        else {
            // The subplan is more complex, so put a filter node above it.
            plan = new SimpleFilterNode(plan, predicate);
        }

        return plan;
    }
}
