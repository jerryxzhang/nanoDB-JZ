package edu.caltech.test.nanodb.indexes.bitmap;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.bitmapfile.BitmapIndexScanNode;
import edu.caltech.test.nanodb.sql.SqlTestCase;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.io.File;


/**
 * Tests the bitmapIndexScanNode class directly
 */
@Test
public class TestBitmapIndexScanNode extends SqlTestCase {
    private static Logger logger = Logger.getLogger(TestBitmapIndexScanNode.class);

    public TestBitmapIndexScanNode() {
        super("setup_testBitmapIndexes");
    }

    public static String tableName = "TEST_BITMAP_INDEXES";
    public static String indexColumn = "B";

    public void testSimpleEquality() throws Throwable {
        File dir = server.getStorageManager().getBaseDir();
        File[] filesList = dir.listFiles();
        for (File file : filesList) {
            if (file.isFile()) {
                logger.info(file.toString());
            }
        }

        logger.info(server.getStorageManager().getFileManager().fileExists(tableName + ".tbl"));

        Expression pred = new CompareOperator(CompareOperator.Type.EQUALS,
                new ColumnValue(new ColumnName(tableName, indexColumn)),
                new LiteralValue(10));

        BitmapIndexScanNode node = new BitmapIndexScanNode(pred,
                server.getStorageManager().getTableManager().openTable(tableName),
                server.getStorageManager().getBitmapIndexManager());
        node.prepare();
        node.initialize();

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

    }

    public void testOr() throws Throwable {

    }

    public void testNot() throws Throwable {

    }
}
