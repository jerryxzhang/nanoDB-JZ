package edu.caltech.nanodb.indexes.bitmapindex;

import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.bitmapfile.Bitmap;
import edu.caltech.nanodb.storage.bitmapfile.BitmapFileManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Handles creating and opening bitmap indexes, as well as some other simple operations on
 * bitmap indexes.
 */
public class BitmapIndexManager {

    private static Logger logger = Logger.getLogger(BitmapIndexManager.class);

    private BitmapFileManager bitmapFileManager;

    private HashMap<Map.Entry<TableInfo, String>, BitmapIndex> cache;

    public BitmapIndexManager(StorageManager storageManager) {
        this.bitmapFileManager = new BitmapFileManager(storageManager);
        cache = new HashMap<Map.Entry<TableInfo, String>, BitmapIndex>();
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
        logger.info("Successfully created bitmap index on " + tableInfo.getTableName() + " : " + attribute + " of size " + bitmapIndex.size());

        // Keep the new index in the cache
        cache.put(new AbstractMap.SimpleEntry<TableInfo, String>(tableInfo, attribute), bitmapIndex);
        return bitmapIndex;
    }

    /**
     * Check whether a bitmap index exists for a given attribute
     */
    public boolean bitmapIndexExists(TableSchema schema, String attribute) {
        int index = schema.getColumnIndex(attribute);
        Iterator<ColumnRefs> iter = schema.getBitmapIndexes().values().iterator();
        while (iter.hasNext()) {
            if (iter.next().getCol(0) == index) return true;
        }
        return false;
    }

    /**
     * Drops a bitmap index
     */
    public void dropBitmapIndex(TableInfo tableInfo, String attribute) {
        // TODO Can't drop indexes yet
    }

    /**
     * Get the existence bitmap for a table. This bitmap can come from any of the bitmap indexes
     * on the table since they should all be the same
     */
    public Bitmap getExistenceBitmap(TableInfo table) {
        Map<String, ColumnRefs> bitmaps = table.getSchema().getBitmapIndexes();
        if (bitmaps.size() == 0) throw new IllegalArgumentException("No bitmaps on this table");
        ColumnRefs columns = bitmaps.values().iterator().next();
        String attribute = table.getSchema().getColumnInfo(columns.getCol(0)).getName();
        return openBitmapIndex(table, attribute).getExistence();
    }

    /**
     * Load an existing bitmap index from cache if possible, otherwise opens from file
     */
    public BitmapIndex openBitmapIndex(TableInfo tableInfo, String attribute) {
        Map.Entry<TableInfo, String> key = new AbstractMap.SimpleEntry<TableInfo, String>(tableInfo, attribute);
        BitmapIndex ret;
        if (cache.containsKey(key)) {
            ret = cache.get(key);
        } else {
            ret = new BitmapIndex(tableInfo, attribute, this);
            ret.load();
            cache.put(key, ret);
            logger.info("Opened bitmap index on " + tableInfo.getTableName() + " : " + attribute + " of size " + ret.size());
        }
        return ret;
    }

}
