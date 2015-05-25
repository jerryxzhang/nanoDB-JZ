package edu.caltech.nanodb.plans;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.indexes.bitmapindex.BitmapIndexManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;

/**
 * A Join node that makes use of bitmap indexes. One of the children must be accessible with bitmap
 * indexes, i.e. it must be a FileScan or BitmapScan, and bitmaps must exist on some of the
 * attributes in the join predicate. The node does not use this child directly, but rather
 * uses bitmap index scans to access the table instead.
 */
public class BitmapIndexJoinNode extends NestedLoopsJoinNode {

    private static Logger logger = Logger.getLogger(BitmapIndexJoinNode.class);

    private BitmapIndexManager bitmapIndexManager;

    private BitmapIndexScanNode rightBitmapNode;

    private TableInfo rightTable;

    /* An extra expression not part of the join expression */
    private Expression rightExtraExpression;

    boolean valid;

    public BitmapIndexJoinNode(PlanNode left, PlanNode right, JoinType joinType, Expression predicate, BitmapIndexManager manager) {
        super(left, right, joinType, predicate);
        bitmapIndexManager = manager;
        rightExtraExpression = null;
        valid = true;

        // TODO if both left and right have indexes, choose the smaller table to be on the left

        // Attempt to set up a bitmap index on the right side
        if (rightChild instanceof FileScanNode) {
            rightTable = ((FileScanNode) rightChild).getTableInfo();
        } else if (rightChild instanceof BitmapIndexScanNode) {
            rightTable = ((BitmapIndexScanNode) rightChild).getTableInfo();
        }

        // If it can't be solved the previous way, try again
        if (!canSolvePredicate(predicate)) {
            swap();

        }

        if (rightChild instanceof FileScanNode) {
            rightTable = ((FileScanNode) rightChild).getTableInfo();
        } else if (rightChild instanceof BitmapIndexScanNode) {
            rightTable = ((BitmapIndexScanNode) rightChild).getTableInfo();
        }

        // If it can't be solved, then its invalid
        if (canSolvePredicate(predicate)) {
            if (rightChild instanceof FileScanNode) {
                rightBitmapNode = new BitmapIndexScanNode(null, null, rightTable, manager);
            } else if (rightChild instanceof BitmapIndexScanNode) {
                try {
                    rightBitmapNode = (BitmapIndexScanNode) rightChild.clone();
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                }
                rightExtraExpression = rightBitmapNode.getPredicate();
            }
        } else {
            valid = false;
        }

        switch (joinType) {
            case INNER:
                break;
            // TODO implement all the following joins. They are theoretically feasible
            case RIGHT_OUTER:

            case LEFT_OUTER:

            case SEMIJOIN:

            case ANTIJOIN:

            case CROSS:
                throw new RuntimeException("Not yet implemented");
            default:
                throw new IllegalArgumentException("Can't do bitmap joins with " + joinType);
        }
    }

    /**
     * Returns whether the given predicate can be solved as part of a join. Each piece of the predicate
     * has to be a compare operation where one element is an attribute on the right table with a bitmap
     * index on that attribute, and the other element an attribute of the left table or a literal.
     */
    public boolean canSolvePredicate(Expression expression) {
        if (rightTable == null) return false;

        Boolean ret = true;
        if (expression instanceof BooleanOperator) {
            BooleanOperator booleanOperator = (BooleanOperator) expression;
            int numTerms = booleanOperator.getNumTerms();
            for (int i = 0; i < numTerms; i++) {
                ret &= canSolvePredicate(booleanOperator.getTerm(i));
            }
        } else if (expression instanceof CompareOperator) {
            Set<String> leftColumns = leftChild.getSchema().getColumnNames();
            Set<String> rightColumns = rightChild.getSchema().getColumnNames();

            CompareOperator compareOperator = (CompareOperator) expression;
            Expression left = compareOperator.getLeftExpression();
            Expression right = compareOperator.getRightExpression();

            // Currently, we allow expressions in format literal = rightAttr or
            // leftAttr = rightAttr. rightAttr must have a bitmap index on it

            // If left is a rightAttr, swap the sides
            if (left instanceof ColumnValue) {
                String name = ((ColumnValue) left).getColumnName().getColumnName();
                if (rightColumns.contains(name)) {
                    Expression temp = left;
                    left = right;
                    right = temp;
                }
            }

            // If right is rightAttr, check that left is leftAttr or Literal, otherwise false
            if (right instanceof ColumnValue) {
                String name = ((ColumnValue) right).getColumnName().getColumnName();
                if (rightColumns.contains(name)) {
                    ret &= (left instanceof LiteralValue) ||
                            (left instanceof ColumnValue && leftColumns.contains(((ColumnValue) left).getColumnName().getColumnName()));
                    ret &= bitmapIndexManager.bitmapIndexExists(rightTable.getSchema(), name);
                } else {
                    ret = false;
                }
            } else {
                ret = false;
            }
        } else {
            ret = false;
        }
        return ret;
    }

    @Override
    protected boolean getTuplesToJoin() throws IOException {
        if (leftTuple == null && !done) {
            leftTuple = leftChild.getNextTuple();
            if (leftTuple == null) {
                done = true;
                return !done;
            }
            environment.clear();
            environment.addTuple(leftSchema, leftTuple);
            Expression newpred = PredicateUtils.makePredicate(PredicateUtils.partiallyEvaluate(predicate, environment), rightExtraExpression);
            rightBitmapNode.setPredicate(newpred);
        }

        rightTuple = rightBitmapNode.getNextTuple();
        while (rightTuple == null) {
            // Reached end of right tuples. Need to get the next left tuple, substitute its values
            // into the predicate, and set the right child to the new predicate.
            leftTuple = leftChild.getNextTuple();
            if (leftTuple == null) {
                // Reached end of left relation.  All done.
                done = true;
                return !done;
            }

            environment.clear();
            environment.addTuple(leftSchema, leftTuple);
            Expression newpred = PredicateUtils.makePredicate(PredicateUtils.partiallyEvaluate(predicate, environment), rightExtraExpression);
            rightBitmapNode.setPredicate(newpred);

            rightTuple = rightBitmapNode.getNextTuple();
        }

        return !done;
    }

    @Override
    protected boolean canJoinTuples() {
        // TODO eventually handle extra predicates on the right side
        return true;
    }

    //TODO mark and reset
    @Override
    public void markCurrentPosition() {

    }

    @Override
    public void resetToLastMark() {

    }

    public boolean isValid() {
        return valid;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("BitmapIndexJoin[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped) ");

        buf.append(']');

        return buf.toString();
    }

}
