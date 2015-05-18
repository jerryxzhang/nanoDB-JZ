package edu.caltech.test.nanodb.indexes.bitmap;

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

    CommandResult simpleSelect1;

    public TestBitmapIndexQueries() throws Throwable {
        super("setup_testBitmapIndexes");
        simpleSelect1 = server.doCommand("", true);
    }

    public void testSimpleSelects() throws Throwable {

    }

    public void testPartialSelects() throws Throwable {

    }

    public void testJoins() throws Throwable {

    }

    public void testAggregations() throws Throwable {

    }
}
