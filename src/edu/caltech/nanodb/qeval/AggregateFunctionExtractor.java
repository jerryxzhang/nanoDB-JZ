package edu.caltech.nanodb.qeval;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.ExpressionProcessor;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;


/**
 * This expression-processor implementation looks for aggregate function
 * calls within an expression, extracts them and gives them a name, then
 * replaces the aggregate calls with column-lookups using the generated
 * names.
 *
 * @todo This class needs to be updated to combine duplicated aggregate
 *       expressions, so that they are only computed once and then reused.
 */
public class AggregateFunctionExtractor implements ExpressionProcessor {

    /**
     * A mapping from generated string names to the corresponding
     * aggregate functions that have been replaced with those names.
     */
    private HashMap<String, FunctionCall> aggregateCalls =
        new HashMap<String, FunctionCall>();


    /**
     * The "current aggregate function" as the expression is being
     * traversed, which will only be non-{@code null} when an aggregate
     * and its arguments are actually being traversed.
     */
    private FunctionCall aggregate = null;


    /**
     * A flag used to track whether aggregate expressions were found while
     * traversing the expression tree.
     */
    private boolean found = false;


    /**
     * This method identifies {@link FunctionCall} objects whose function
     * is an {@link AggregateFunction}, and then records these into the
     * {@link #aggregate} field so that they can be processed by the
     * {@link #leave} method.
     *
     * @param e the expression node being entered
     *
     * @throws IllegalArgumentException if a nested aggregate function is
     *         encountered within the expression.
     */
    public void enter(Expression e) {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                if (aggregate != null) {
                    // Error:  Nested aggregates aren't allowed!
                    throw new IllegalArgumentException(
                        "Found aggregate function call " + call +
                        " nested within another aggregate call " + aggregate);
                }
                aggregate = call;
                found = true;
            }
        }
    }


    /**
     * This method identifies {@link FunctionCall} objects whose function
     * is an {@link AggregateFunction}, generates a name for the
     * aggregate, then replaces the aggregate call with a column-access to
     * the generated name.
     *
     * @param e the expression node being left
     *
     * @return the replacement {@link ColumnValue} for an aggregate function,
     *         if an aggregate function call is being left, or the original
     *         expression node {@code e} otherwise.
     */
    public Expression leave(Expression e) {
        if (e instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) e;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                if (aggregate != call) {
                    // This would be a bug.
                    throw new IllegalStateException(
                        "Expected to find aggregate " + aggregate +
                        " but found " + call + " instead.");
                }

                // We will compute the aggregate separately, so replace
                // the aggregate call with a placeholder column name.

                String name = "#AGG" + (aggregateCalls.size() + 1);
                aggregateCalls.put(name, aggregate);
                aggregate = null;

                // This is the replacement.
                ColumnName colName = new ColumnName(name);
                return new ColumnValue(colName);
            }
        }
        return e;
    }


    public boolean foundAggregates() {
        return found;
    }


    public void clearFoundFlag() {
        found = false;
    }


    public Map<String, FunctionCall> getAggregateCalls() {
        return Collections.unmodifiableMap(aggregateCalls);
    }
}
