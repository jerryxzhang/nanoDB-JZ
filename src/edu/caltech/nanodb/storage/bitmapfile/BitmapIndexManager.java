package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Handles creating and opening bitmap indexes.
 */
public class BitmapIndexManager {

    private static Logger logger = Logger.getLogger(BitmapIndexManager.class);

    private BitmapFileManager bitmapFileManager;

    public BitmapIndexManager(StorageManager storageManager) {
        this.bitmapFileManager = new BitmapFileManager(storageManager);
    }

    public BitmapFileManager getBitmapFileManager() {
        return bitmapFileManager;
    }

    /**
     * Create and populate a new bitmap index, and write the bitmap index info to the schema
     */
    public BitmapIndex createBitmapIndex(TableInfo tableInfo, ColumnRefs columnRefs) {
        BitmapIndex bitmapIndex = null;
        int[] columns = columnRefs.getCols();
        if (columns.length != 1) throw new IllegalArgumentException("Bitmap indices can only be on one attribute");
        String attribute = tableInfo.getSchema().getColumnInfo(columns[0]).getName();

        String indexName = columnRefs.getIndexName();

        // Give the index a simple default name
        if (indexName == null) {
            indexName = tableInfo.getTableName() + attribute;
            columnRefs.setIndexName(indexName);
        }

        if (tableInfo.getSchema().getBitmapIndexes().containsValue(indexName))
            throw new IllegalArgumentException("Already exists bitmap index with same name");

        try {
            bitmapIndex = new BitmapIndex(tableInfo, attribute, this);
            bitmapIndex.populate();

            // Indicate in table schema that index exists
            tableInfo.getSchema().addBitmapIndex(columnRefs);
            bitmapFileManager.getStorageManager().getTableManager().saveTableInfo(tableInfo);
        } catch (IOException e) {
            logger.error("Failed to create new bitmap index " + indexName);
        }
        return bitmapIndex;
    }

    public boolean bitmapIndexExists(TableSchema schema, String attribute) {
        int index = schema.getColumnIndex(attribute);
        Iterator<ColumnRefs> iter = schema.getBitmapIndexes().values().iterator();
        while (iter.hasNext()) {
            if (iter.next().getCol(0) == index) return true;
        }
        return false;
    }

    public void dropBitmapIndex(TableInfo tableInfo, String attribute) {

    }

    public Bitmap getExistenceBitmap(TableInfo table) {
        Map<String, ColumnRefs> bitmaps = table.getSchema().getBitmapIndexes();
        if (bitmaps.size() == 0) throw new IllegalArgumentException("No bitmaps on this table");
        ColumnRefs columns = bitmaps.values().iterator().next();
        String attribute = table.getSchema().getColumnInfo(columns.getCol(0)).getName();
        return openBitmapIndex(table, attribute).getExistence();
    }

    /**
     * Load an existing bitmap index
     */
    public BitmapIndex openBitmapIndex(TableInfo tableInfo, String attribute) {
        BitmapIndex bitmapIndex = new BitmapIndex(tableInfo, attribute, this);
        bitmapIndex.load();
        return bitmapIndex;
    }

}
