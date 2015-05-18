package edu.caltech.nanodb.plans;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.indexes.bitmapindex.BitmapIndexManager;

import java.io.IOException;

/**
 * A Join node that makes use of bitmap indexes. One of the children must be accessible with bitmap
 * indexes, i.e. it must be a FileScan or BitmapScan, and bitmaps must exist on some of the
 * attributes in the join predicate. The node does not use this child at all, but rather
 * uses bitmap index scans to access the table instead.
 */
public class BitmapIndexJoinNode extends NestedLoopsJoinNode {

    private BitmapIndexManager bitmapIndexManager;

    private BitmapIndexScanNode rightBitmapNode;

    public BitmapIndexJoinNode(PlanNode leftChild, PlanNode rightChild, JoinType joinType, Expression predicate, BitmapIndexManager manager) {
        super(leftChild, rightChild, joinType, predicate);
        bitmapIndexManager = manager;

        // TODO If both children are file / bitmap scans pick one to be the left that is more efficient
        if (!(rightChild instanceof FileScanNode || rightChild instanceof BitmapIndexScanNode)) {
            swap();
        }

        if (!(rightChild instanceof FileScanNode || rightChild instanceof BitmapIndexScanNode)) {
            throw new IllegalArgumentException("One side must be a direct table scan using File or Bitmap");
        }

        switch (joinType) {
            case INNER:
                break;
            // TODO implement all the following joins. They are theoretically feasible
            /* case RIGHT_OUTER:
                break;
            case LEFT_OUTER:
                break;
            case SEMIJOIN:
                break;
            case ANTIJOIN:
                break;
            case CROSS:
                break; */
            default:
                throw new IllegalArgumentException("Can't do bitmap joins with " + joinType);
        }

    }

    public boolean canSolvePredicate(PlanNode child1, PlanNode child2, Expression predicate) {
        return false;
    }

    @Override
    public Tuple getNextTuple() throws IllegalStateException, IOException {
        if (done)
            return null;

        leftTuple = leftChild.getNextTuple();
        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
     //   environment.getColumnValue();

        return null;
    }

    @Override
    public void markCurrentPosition() {

    }

    @Override
    public void resetToLastMark() {

    }

    @Override
    public String toString() {
        return null;
    }

}
