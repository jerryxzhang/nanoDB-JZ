package edu.caltech.nanodb.indexes.bitmapindex;

import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.RowEventListener;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Allows updating bitmap indexes when a row is modified in the table
 */
public class BitmapIndexUpdater implements RowEventListener {

    private static Logger logger = Logger.getLogger(BitmapIndexUpdater.class);

    private BitmapIndexManager bitmapIndexManager;

    public BitmapIndexUpdater(StorageManager manager) {
        this.bitmapIndexManager = manager.getBitmapIndexManager();
    }

    @Override
    public void beforeRowInserted(TableInfo tblFileInfo, Tuple newValues) {
        // Ignore.
    }

    @Override
    public void afterRowInserted(TableInfo tblFileInfo, Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                    "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToBitmapIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowUpdated(TableInfo tblFileInfo, Tuple oldTuple,
                                 Tuple newValues) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                    "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromBitmapIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowUpdated(TableInfo tblFileInfo, Tuple oldValues,
                                Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                    "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToBitmapIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowDeleted(TableInfo tblFileInfo, Tuple oldTuple) {
        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                    "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromBitmapIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowDeleted(TableInfo tblFileInfo, Tuple oldValues) {
        // Ignore.
    }

    private void addRowToBitmapIndexes(TableInfo info, PageTuple tuple) {
        Iterator<ColumnRefs> iter = info.getSchema().getBitmapIndexes().values().iterator();
        while (iter.hasNext()) {
            ColumnRefs refs = iter.next();
            String attribute = info.getSchema().getColumnInfo(refs.getCol(0)).getName();
            BitmapIndex index = bitmapIndexManager.openBitmapIndex(info, attribute);
            index.addTuple(tuple);
        }
        // Iterate through indexes on the table
    }

    private void removeRowFromBitmapIndexes(TableInfo info, PageTuple tuple) {
        Iterator<ColumnRefs> iter = info.getSchema().getBitmapIndexes().values().iterator();
        while (iter.hasNext()) {
            ColumnRefs refs = iter.next();
            String attribute = info.getSchema().getColumnInfo(refs.getCol(0)).getName();
            BitmapIndex index = bitmapIndexManager.openBitmapIndex(info, attribute);
            index.removeTuple(tuple);
        }
    }
}
