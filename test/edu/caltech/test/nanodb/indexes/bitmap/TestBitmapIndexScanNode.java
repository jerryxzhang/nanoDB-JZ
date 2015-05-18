package edu.caltech.test.nanodb.indexes.bitmap;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.plans.BitmapIndexScanNode;
import edu.caltech.test.nanodb.sql.SqlTestCase;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.util.HashSet;


/**
 * Tests the bitmapIndexScanNode class without going through server commands. Also tests
 * finding tuples with pointers.
 */
@Test
public class TestBitmapIndexScanNode extends SqlTestCase {
    private static Logger logger = Logger.getLogger(TestBitmapIndexScanNode.class);

    public TestBitmapIndexScanNode() {
        super("setup_testBitmapIndexes");
    }

    public static String tableName = "TEST_BITMAP_INDEXES";
    public static String column1 = "B";
    public static String column2 = "A";

    public void testSimpleEquality() throws Throwable {
        Expression pred = new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(new ColumnName(tableName, column1)),
                new LiteralValue(10));

        BitmapIndexScanNode node = new BitmapIndexScanNode(pred,
                server.getStorageManager().getTableManager().openTable(tableName),
                server.getStorageManager().getBitmapIndexManager());
        node.prepare();
        node.initialize();

        assert(BitmapIndexScanNode.canProcessExpression(pred, server.getStorageManager().getBitmapIndexManager(),
                server.getStorageManager().getTableManager().openTable(tableName)));

        Tuple tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(1)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(4)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple == null);
    }

    public void testAnd() throws Throwable {
        HashSet<Expression> set = new HashSet<Expression>();
        set.add(new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(new ColumnName(tableName, column1)),
                new LiteralValue(10)));
        set.add(new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(new ColumnName(tableName, column2)),
                new LiteralValue(4)));
        Expression pred = new BooleanOperator(BooleanOperator.Type.AND_EXPR, set);

        assert(BitmapIndexScanNode.canProcessExpression(pred, server.getStorageManager().getBitmapIndexManager(),
                server.getStorageManager().getTableManager().openTable(tableName)));

        BitmapIndexScanNode node = new BitmapIndexScanNode(pred,
                server.getStorageManager().getTableManager().openTable(tableName),
                server.getStorageManager().getBitmapIndexManager());
        node.prepare();
        node.initialize();

        Tuple tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(4)));
        assert (tuple.getColumnValue(1).equals(Integer.valueOf(10)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple == null);
    }

    public void testOr() throws Throwable {
        HashSet<Expression> set = new HashSet<Expression>();
        set.add(new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(new ColumnName(tableName, column1)),
                new LiteralValue(10)));
        set.add(new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(new ColumnName(tableName, column2)),
                new LiteralValue(3)));
        Expression pred = new BooleanOperator(BooleanOperator.Type.OR_EXPR, set);

        assert(BitmapIndexScanNode.canProcessExpression(pred, server.getStorageManager().getBitmapIndexManager(),
                server.getStorageManager().getTableManager().openTable(tableName)));

        BitmapIndexScanNode node = new BitmapIndexScanNode(pred,
                server.getStorageManager().getTableManager().openTable(tableName),
                server.getStorageManager().getBitmapIndexManager());
        node.prepare();
        node.initialize();

        Tuple tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(1)));
        assert (tuple.getColumnValue(1).equals(Integer.valueOf(10)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(3)));
        assert (tuple.getColumnValue(1).equals(Integer.valueOf(30)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(4)));
        assert (tuple.getColumnValue(1).equals(Integer.valueOf(10)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple == null);
    }

    public void testNot() throws Throwable {
        HashSet<Expression> set = new HashSet<Expression>();
        set.add(new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(new ColumnName(tableName, column1)),
                new LiteralValue(10)));
        Expression pred = new BooleanOperator(BooleanOperator.Type.NOT_EXPR, set);

        assert(BitmapIndexScanNode.canProcessExpression(pred, server.getStorageManager().getBitmapIndexManager(),
                server.getStorageManager().getTableManager().openTable(tableName)));

        BitmapIndexScanNode node = new BitmapIndexScanNode(pred,
                server.getStorageManager().getTableManager().openTable(tableName),
                server.getStorageManager().getBitmapIndexManager());
        node.prepare();
        node.initialize();

        Tuple tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(0)));
        assert (tuple.getColumnValue(1).equals(Integer.valueOf(0)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(2)));
        assert (tuple.getColumnValue(1).equals(Integer.valueOf(20)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple.getColumnValue(0).equals(Integer.valueOf(3)));
        assert (tuple.getColumnValue(1).equals(Integer.valueOf(30)));

        tuple = node.getNextTuple();
        logger.debug(tuple);
        assert (tuple == null);
    }
}
