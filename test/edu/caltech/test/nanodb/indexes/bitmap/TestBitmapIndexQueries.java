package edu.caltech.test.nanodb.indexes.bitmap;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.test.nanodb.sql.SqlTestCase;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;


/**
 * Tests various queries on tables with bitmap indexes. Queries should give the same result as
 * the same query without bitmap indexes.
 */
@Test
public class TestBitmapIndexQueries extends SqlTestCase {
    private static Logger logger = Logger.getLogger(TestBitmapIndexQueries.class);

    private String table = "test_bitmap_indexes";
    private String table_noindex = "test_bitmap_indexes1";
    private String table2 = "test_bitmap_indexes2";

    private String attr1 = "a";
    private String attr2 = "b";
    private String attr3 = "d";


    public TestBitmapIndexQueries() throws Throwable {
        super("setup_testBitmapIndexes");
    }

    public void testSimpleSelects() throws Throwable {
        CommandResult r1 = server.doCommand("SELECT * FROM test_bitmap_indexes WHERE b = 10", true);
        CommandResult r2 = server.doCommand("SELECT * FROM test_bitmap_indexes1 WHERE b = 10", true);

        logger.info(r1.getTuples());
        logger.info(r2.getTuples());

        assert(checkUnorderedResults(r1.getTuples().toArray(new TupleLiteral[r1.getTuples().size()]), r2));

        CommandResult r3 = server.doCommand("SELECT * FROM test_bitmap_indexes WHERE b = 10 OR a = 3", true);
        CommandResult r4 = server.doCommand("SELECT * FROM test_bitmap_indexes1 WHERE b = 10 OR a = 3", true);

        logger.info(r3.getTuples());
        logger.info(r4.getTuples());

        assert(checkUnorderedResults(r3.getTuples().toArray(new TupleLiteral[r3.getTuples().size()]), r4));
    }

    public void testPartialSelects() throws Throwable {
        CommandResult r1 = server.doCommand("SELECT * FROM test_bitmap_indexes WHERE b = 10 AND c = 6", true);
        CommandResult r2 = server.doCommand("SELECT * FROM test_bitmap_indexes1 WHERE b = 10 AND c = 6", true);

        logger.info(r1.getTuples());
        logger.info(r2.getTuples());

        assert(checkUnorderedResults(r1.getTuples().toArray(new TupleLiteral[r1.getTuples().size()]), r2));

        CommandResult r3 = server.doCommand("SELECT * FROM test_bitmap_indexes WHERE (b = 10 OR a = 3) AND c = 6", true);
        CommandResult r4 = server.doCommand("SELECT * FROM test_bitmap_indexes1 WHERE (b = 10 OR a = 3) AND c = 6", true);

        logger.info(r3.getTuples());
        logger.info(r4.getTuples());

        assert(checkUnorderedResults(r3.getTuples().toArray(new TupleLiteral[r3.getTuples().size()]), r4));
    }

    public void testJoins() throws Throwable {
        CommandResult r1 = server.doCommand("SELECT * FROM test_bitmap_indexes NATURAL JOIN test_bitmap_indexes2", true);
        CommandResult r2 = server.doCommand("SELECT * FROM test_bitmap_indexes1 NATURAL JOIN test_bitmap_indexes2", true);

        logger.info(r1.getTuples());
        logger.info(r2.getTuples());

        assert(checkUnorderedResults(r1.getTuples().toArray(new TupleLiteral[r1.getTuples().size()]), r2));

        CommandResult r3 = server.doCommand("SELECT * FROM test_bitmap_indexes NATURAL JOIN test_bitmap_indexes2 WHERE b = 10", true);
        CommandResult r4 = server.doCommand("SELECT * FROM test_bitmap_indexes1 NATURAL JOIN test_bitmap_indexes2 WHERE b = 10", true);

        logger.info(r3.getTuples());
        logger.info(r4.getTuples());

        assert(checkUnorderedResults(r3.getTuples().toArray(new TupleLiteral[r3.getTuples().size()]), r4));
    }

    public void testAggregations() throws Throwable {

    }
}
