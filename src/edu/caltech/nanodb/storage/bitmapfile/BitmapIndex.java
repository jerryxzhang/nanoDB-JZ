package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.heapfile.HeapFilePageTuple;
import edu.caltech.nanodb.storage.heapfile.HeapTupleFile;
import edu.caltech.nanodb.storage.heapfile.HeapTupleFileManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This file represents a complete bitmap index as one would create it through
 * DDL. The index is created on one table and one value. The parts include one existence bitmap,
 * one locator bitmap, and many value bitmaps, one for each distinct value of the attribute.
 * The existence bitmap and locator bitmap are the same for all indexes on the same table, while
 * the value bitmaps are specific to this attribute.
 */
public class BitmapIndex {
    private BitmapIndexManager bitmapIndexManager;
    private StorageManager storageManager;

    /* The table and attribute define the index when the index is created */
    private TableInfo table;
    private String attribute;

    /* These bitmaps are set up when the index is populated */
    private PointerList locator;
    private ValueSet values;
    private Bitmap existence;
    private HashMap<String, Bitmap> valueBitmaps;

    public BitmapIndex(TableInfo table, String attribute, BitmapIndexManager manager) {
        this.table = table;
        this.attribute = attribute;
        this.valueBitmaps = new HashMap<String, Bitmap>();
        this.bitmapIndexManager = manager;
        this.storageManager = manager.getBitmapFileManager().getStorageManager();
        locator = new PointerList(getLocatorFileName(table.getTableName(), attribute), storageManager);
        values = new ValueSet(getValuesFileName(table.getTableName(), attribute), storageManager);
    }

    /**
     * Used on initial creation of an index. Iterates through every tuple in the table and create all
     * necessary bitmaps.
     */
    public void populate() throws IOException {
        // Add support for other file types like BTree or Hash later.
        if (!(table.getTupleFile() instanceof  HeapTupleFile))
            throw new IllegalArgumentException("Can't populate from non heap files yet");

        // Initialize all the index parts
        existence = bitmapIndexManager.getBitmapFileManager()
                .createBitmapFile(getExistenceBitmapName(table.getTableName(), attribute), this);
        locator.createFile();
        values.createFile();

        // Scan through all tuples in the table
        HeapTupleFile heapTupleFile = (HeapTupleFile) table.getTupleFile();
        HeapFilePageTuple tuple = (HeapFilePageTuple) heapTupleFile.getFirstTuple();

        int i = 0;
        while (true) {
            // Set the existence bit
            existence.set(i);

            // If a bitmap for this value exists, set a bit in that bitmap, otherwise create a new
            // one, set the bit, and cache it. Add the value to ValueSet if its not already there
            String value = String.valueOf(tuple.getColumnValue(tuple.getSchema().getColumnIndex(attribute)));
            if (valueBitmaps.containsKey(value)) {
                valueBitmaps.get(value).set(i);
            } else {
                Bitmap map = bitmapIndexManager.getBitmapFileManager()
                        .createBitmapFile(getValueBitmapName(table.getTableName(), attribute, value), this);
                map.set(i);
                valueBitmaps.put(value, map);
                values.addValue(value);
            }

            // Add a FilePointer to this tuple
            locator.addPointer(tuple.getExternalReference());

            // Move to the next tuple
            i += 1;
            tuple = (HeapFilePageTuple) heapTupleFile.getNextTuple(tuple);
            if (tuple == null) break;
        }
    }

    /**
     * Used to load parts of an index from file. Individual bitmaps aren't loaded until they are needed.
     */
    public void load() throws IOException {
        existence = bitmapIndexManager.getBitmapFileManager()
                .loadBitmapFile(getExistenceBitmapName(table.getTableName(), attribute), this);
        locator.loadFile();
        values.loadFile();

        // Store all the values, but don't actually load the bitmaps until they are used
        Iterator<String> iter = values.getValues().iterator();
        while (iter.hasNext()) {
            String value = iter.next();
            valueBitmaps.put(value, null);
        }
    }

    public void update(Tuple tuple) {

    }

    public void insert(Tuple tuple) {

    }

    public void delete(Tuple tuple) {

    }

    public Bitmap getBitmap(String value) {
        // value doesn't exist at all
        if (!valueBitmaps.containsKey(value)) return null;

        // value exists but hasn't been loaded
        if (valueBitmaps.get(value) == null) {
            String name = getValueBitmapName(table.getTableName(), attribute, value);
            Bitmap map = bitmapIndexManager.getBitmapFileManager().loadBitmapFile(name, this);
            valueBitmaps.put(value, map);
            return map;
        }

        return valueBitmaps.get(value);
    }

    public TableInfo getTableInfo() {
        return table;
    }

    public String getAttribute() {
        return attribute;
    }

    // TODO Use URL Encoding on all the below
    public static String getExistenceBitmapName(String table, String attribute) {
        return new StringBuilder("BITMAP__EXISTENCE__").append(table).append("__").append(attribute).toString();
    }

    public static String getLocatorFileName(String table, String attribute) {
        return new StringBuilder("BITMAP__LOCATOR__").append(table).append("__").append(attribute).toString();
    }

    public static String getValuesFileName(String table, String attribute) {
        return new StringBuilder("BITMAP__VALUES__").append(table).append("__").append(attribute).toString();
    }

    /**
     * Encode the table, attribute, and value for a bitmap into a single string.
     */
    public static String getValueBitmapName(String table, String attr, String value) {
        return new StringBuilder("BITMAP__").append(table).append("__").append(attr).append("__").append(value).toString();
    }


}
