package edu.caltech.nanodb.storage.bitmapfile;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.relations.TableInfo;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.TupleFile;



/**
 * A Bitmap Index Scan node. Allows a predicate composed of column equalities
 * to be evaluated very quickly when on a table with bitmap indexes.
 */
public class BitmapIndexScanNode extends SelectNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BitmapIndexScanNode.class);

    private BitmapIndexManager bitmapIndexManager;

    private TableInfo tableInfo;

    /** The table that the index is built against. */
    private TupleFile tableTupleFile;

    private FilePointer[] pointers;

    /**
     * This field allows the index-scan node to mark a particular tuple in the
     * tuple-stream and then rewind to that point in the tuple-stream.
     */
    private int markedTupleIndex;

    private int currentTupleIndex;

    private boolean jumpToMarkedTuple;


    /**
     * Construct an index scan node that performs an equality-based lookup on
     * an index.
     */
    public BitmapIndexScanNode(Expression predicate, TableInfo table, BitmapIndexManager manager) {
        super(predicate);
        this.tableInfo = table;
        this.tableTupleFile = table.getTupleFile();
        this.bitmapIndexManager = manager;

        // Evaluate the expression into a bitmap and pull file pointers from it1
        Bitmap bitmap = processExpression(predicate);
        pointers = new FilePointer[bitmap.cardinality()];
        int[] indexes = bitmap.toArray();
        for (int i = 0; i < pointers.length; i++) {
            pointers[i] = BitmapIndex.getPointer(indexes[i]);
        }
    }

    /**
     * Given an expression that can be evaluated through Bitmap indexes, return the Bitmap
     * that represents the tuples in the evaluated expression.
     * <p>
     * So far only the following can be processed by Bitmap indexes:
     * <ul>
     *     <li>AND, OR, and NOT expression composed of expressions that can be evaluated
     *     by bitmaps</li>
     *     <li>An = or != expression comparing a column value to a literal value where
     *     there exists a bitmap index on the column</li>
     * </ul>
     */
    public Bitmap processExpression(Expression expression) {
        logger.info(expression);
        Bitmap ret;
        if (expression instanceof BooleanOperator) {
            // If the expression is an and, or, or not then break it down to its components
            // and build the bitmaps with boolean operators
            BooleanOperator booleanOperator = (BooleanOperator) expression;
            int numTerms = booleanOperator.getNumTerms();
            switch (booleanOperator.getType()) {
                case AND_EXPR:
                    ret = processExpression(booleanOperator.getTerm(0));
                    for (int i = 1; i < numTerms; i++)
                        ret = Bitmap.and(ret, processExpression(booleanOperator.getTerm(i)));
                    break;
                case OR_EXPR:
                    ret = processExpression(booleanOperator.getTerm(0));
                    for (int i = 1; i < numTerms; i++)
                        ret = Bitmap.or(ret, processExpression(booleanOperator.getTerm(i)));
                    break;
                case NOT_EXPR:
                    // The NOT operation is replicated by taking the xor of the existence bitmap with this bitmap
                    ret = Bitmap.xor(processExpression(booleanOperator.getTerm(0)), bitmapIndexManager.getExistenceBitmap(tableInfo));
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized boolean type");
            }
        } else if (expression instanceof CompareOperator) {
            // Check whether expression is an equality with a literal
            CompareOperator compareOperator = (CompareOperator) expression;
            Expression left = compareOperator.getLeftExpression();
            Expression right = compareOperator.getRightExpression();

            if (left instanceof LiteralValue && right instanceof ColumnValue) {
                // Swap left and right
                Expression temp = right;
                right = left;
                left = temp;
            }

            if (!(right instanceof LiteralValue && left instanceof ColumnValue))
                throw new IllegalArgumentException("Can only process columns and literals");

            // Check if an index actually exists on this column
            String columnName = ((ColumnValue) left).getColumnName().getColumnName();
            if (!bitmapIndexManager.bitmapIndexExists(tableInfo.getSchema(), columnName))
                throw new IllegalArgumentException("No bitmap index on this column");

            String value = String.valueOf(right.evaluate());

            // Construct the bitmap result
            switch (compareOperator.getType()) {
                case EQUALS:
                    ret = bitmapIndexManager.openBitmapIndex(tableInfo, columnName).getBitmap(value);
                    break;
                case NOT_EQUALS:
                    // Add a NOT operation over the equals
                    ret = bitmapIndexManager.openBitmapIndex(tableInfo, columnName).getBitmap(value);
                    ret = Bitmap.xor(ret, bitmapIndexManager.getExistenceBitmap(tableInfo));
                    break;
                default:
                    throw new UnsupportedOperationException("Can't do other comparisons with bitmaps");
            }
        } else {
            throw new IllegalArgumentException("Can't use bitmaps with this expression");
        }
        return ret;
    }

    /**
     * Returns true if the passed-in object is a <tt>BitmapIndexScanNode</tt> with
     * the same predicate and table.
     *
     * @param obj the object to check for equality
     *
     * @return true if the passed-in object is equal to this object; false
     *         otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BitmapIndexScanNode) {
            BitmapIndexScanNode other = (BitmapIndexScanNode) obj;
            return tableInfo == other.tableInfo && predicate == other.predicate;
        }
        return false;
    }


    /**
     * Computes the hashcode of a PlanNode.  This method is used to see if two
     * plan nodes CAN be equal.
     **/
    public int hashCode() {
        int hash = 7;
        hash = hash * 13 + tableInfo.hashCode();
        hash = hash * 17 + predicate.hashCode();
        return hash;
    }


    /**
     * Creates a copy of this simple filter node node and its subtree.  This
     * method is used by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        BitmapIndexScanNode node = (BitmapIndexScanNode) super.clone();
        node.bitmapIndexManager = bitmapIndexManager;
        node.tableInfo = tableInfo;
        node.tableTupleFile = tableTupleFile;
        node.pointers = pointers;
        return node;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("BitmapIndexScan[");
        buf.append("TABLE: ");
        buf.append(tableInfo.getTableName());
        buf.append("; PREDICATE: ");
        buf.append(predicate);
        buf.append("]");

        return buf.toString();
    }


    /**
     * Unsorted results.
     */
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }

    /** This node supports marking. */
    public boolean supportsMarking() {
        return true;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresRightMarking() {
        return false;
    }

    // Inherit javadocs from base class.
    public void prepare() {
        // Grab the schema and statistics from the table file.

        schema = tableTupleFile.getSchema();

        // TODO:  We should also update the table statistics based on what the
        //        index scan is going to do, but that's too complicated, so
        //        we'll leave them unchanged for now.
        TableStats tableStats = tableTupleFile.getStats();
        stats = tableStats.getAllColumnStats();

        // TODO:  Cost the plan node
        cost = null;
    }

    @Override
    public void initialize() {
        super.initialize();

        currentTupleIndex = 0;
        // Reset our marking state.
        jumpToMarkedTuple = false;
    }

    @Override
    protected void advanceCurrentTuple() throws IllegalStateException, IOException {
        try {
            if (jumpToMarkedTuple) {
                currentTupleIndex = markedTupleIndex;
                jumpToMarkedTuple = false;
                currentTuple = tableTupleFile.getTuple(pointers[currentTupleIndex]);
            } else {
                if (currentTupleIndex == 0 && currentTuple == null) {
                    currentTuple = tableTupleFile.getTuple(pointers[currentTupleIndex]);
                } else if (currentTupleIndex == pointers.length - 1) {
                    currentTuple = null;
                } else {
                    currentTupleIndex += 1;
                    currentTuple = tableTupleFile.getTuple(pointers[currentTupleIndex]);
                }
            }
        } catch (InvalidFilePointerException e) {
            logger.error("Invalid file pointer!!!");
        }
    }

    public void cleanUp() {
        // Nothing to do!
    }


    public void markCurrentPosition() {
        logger.debug("Marking current position in tuple-stream.");
        markedTupleIndex = currentTupleIndex;
    }


    public void resetToLastMark() {
        logger.debug("Resetting to previously marked position in tuple-stream.");
        jumpToMarkedTuple = true;
    }
}
