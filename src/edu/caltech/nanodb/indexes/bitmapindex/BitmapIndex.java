package edu.caltech.nanodb.indexes.bitmapindex;

import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.*;
import edu.caltech.nanodb.storage.bitmapfile.Bitmap;
import edu.caltech.nanodb.storage.bitmapfile.ValueSet;
import edu.caltech.nanodb.storage.heapfile.HeapFilePageTuple;
import edu.caltech.nanodb.storage.heapfile.HeapTupleFile;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This file represents a single complete bitmap index with one table and one attribute.
 * The parts include one existence bitmap and many value bitmaps, one for each distinct
 * value of the attribute.
 */
public class BitmapIndex {
    private BitmapIndexManager bitmapIndexManager;
    private StorageManager storageManager;

    /* The table and attribute define the index when the index is created */
    private TableInfo table;
    private String attribute;

    /* These bitmaps are set up when the index is populated */
    private ValueSet values;
    private Bitmap existence;
    private HashMap<String, Bitmap> valueBitmaps;

    /* Precomputed values */
    private static int bitsPerPage = StorageManager.getCurrentPageSize() / 2;

    public BitmapIndex(TableInfo table, String attribute, BitmapIndexManager manager) {
        this.table = table;
        this.attribute = attribute;
        this.valueBitmaps = new HashMap<String, Bitmap>();
        this.bitmapIndexManager = manager;
        this.storageManager = manager.getBitmapFileManager().getStorageManager();
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
        values.createFile();

        // Scan through all tuples in the table
        HeapTupleFile heapTupleFile = (HeapTupleFile) table.getTupleFile();
        PageTuple tuple = (PageTuple) heapTupleFile.getFirstTuple();

        while (true) {
            if (tuple == null) break;
            int locationBit = getIndex(tuple.getExternalReference());

            // Set the existence bit
            existence.set(locationBit);

            // If a bitmap for this value exists, set a bit in that bitmap, otherwise create a new
            // one, set the bit, and cache it. Add the value to ValueSet if its not already there
            String value = String.valueOf(tuple.getColumnValue(tuple.getSchema().getColumnIndex(attribute)));
            if (valueBitmaps.containsKey(value)) {
                valueBitmaps.get(value).set(locationBit);
            } else {
                createBitmap(value).set(locationBit);
            }

            // Move to the next tuple
            tuple = (HeapFilePageTuple) heapTupleFile.getNextTuple(tuple);
        }
    }

    /**
     * Used to load parts of an index from file. Individual bitmaps aren't loaded until they are needed.
     */
    public void load() {
        existence = bitmapIndexManager.getBitmapFileManager()
                .loadBitmapFile(getExistenceBitmapName(table.getTableName(), attribute), this);
        values.loadFile();

        // Store all the values, but don't actually load the bitmaps until they are used
        Iterator<String> iter = values.getValues().iterator();
        while (iter.hasNext()) {
            String value = iter.next();
            valueBitmaps.put(value, null);
        }
    }

    /**
     * Add a new tuple to this index and updates all necessary bitmaps
     */
    public void addTuple(PageTuple tuple) {
        int location = getIndex(tuple.getExternalReference());

        existence.set(location);
        String value = String.valueOf(tuple.getColumnValue(table.getSchema().getColumnIndex(attribute)));
        Bitmap bitmap = getBitmap(value);
        if (bitmap == null) bitmap = createBitmap(value);
        bitmap.set(location);
    }

    /**
     * Removes a tuple from this index and updates the existence bitmap
     */
    public void removeTuple(PageTuple tuple) {
        int location = getIndex(tuple.getExternalReference());

        if (!existence.contains(location)) throw new IllegalArgumentException("Never existed????");
        existence.unset(location);

        String value = String.valueOf(tuple.getColumnValue(table.getSchema().getColumnIndex(attribute)));

        // Not strictly necessary to unset here, but it lets us check if there are no values left
        // this.valueBitmaps.get(value).unset(location);
        // TODO ?? if there are no values left, remove this bitmap and remove the value from valuelist
    }

    /**
     * Drops this index by deleting all files associated with it
     */
    public void drop() {
        this.existence = null;
        try {
            storageManager.getFileManager().deleteDBFile(getExistenceBitmapName(table.getTableName(), attribute));
            storageManager.getFileManager().deleteDBFile(getValuesFileName(table.getTableName(), attribute));
            Iterator<String> iter = values.getValues().iterator();
            while (iter.hasNext()) {
                String value = iter.next();
                storageManager.getFileManager().deleteDBFile(getValueBitmapName(table.getTableName(), attribute, value));
            }
            this.values = null;
            this.valueBitmaps = null;
        } catch (IOException e) {

        }
    }

    /**
     * Gets the bitmap for a value, opening it if necessary
     */
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

    /**
     * Creates a new bitmap for this value and stores it in the map
     */
    public Bitmap createBitmap(String value) {
        if (valueBitmaps.containsKey(value)) throw new IllegalArgumentException("Already exists!");

        values.addValue(value);
        String name = getValueBitmapName(table.getTableName(), attribute, value);
        Bitmap map =  bitmapIndexManager.getBitmapFileManager().createBitmapFile(name, this);
        valueBitmaps.put(value, map);
        return map;
    }

    /**
     * Translate a filepointer to an index in a bitmap
     */
    public static int getIndex(FilePointer fp) {
        return fp.getPageNo() * bitsPerPage + fp.getOffset();
    }

    /**
     * Translates an index in a bitmap to a Filepointer
     */
    public static FilePointer getPointer(int index) {
        return new FilePointer(index / bitsPerPage, index % bitsPerPage);
    }

    public TableInfo getTableInfo() {
        return table;
    }

    public String getAttribute() {
        return attribute;
    }

    public Bitmap getExistence() {
        return existence;
    }

    /**
     * Returns the number of bytes the entire index takes up on disk, at minimum. This does not take
     * into account whatever extra space DBFiles use. For testing only.
     */
    public int size() {
        int ret = existence.save().length;
        Iterator<String> iter = valueBitmaps.keySet().iterator();
        while (iter.hasNext()) {
            ret += getBitmap(iter.next()).save().length;
        }
        return ret;
    }

    public static String getExistenceBitmapName(String table, String attribute) {
        try {
            table = URLEncoder.encode(table, "UTF-8");
            attribute = URLEncoder.encode(attribute, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return new StringBuilder(table).append("()").append(attribute).append(".bitmap").toString();
    }

    public static String getValuesFileName(String table, String attribute) {
        try {
            table = URLEncoder.encode(table, "UTF-8");
            attribute = URLEncoder.encode(attribute, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return new StringBuilder(table).append("()").append(attribute).append(".values").toString();
    }

    /**
     * Encode the table, attribute, and value for a bitmap into a single string.
     */
    public static String getValueBitmapName(String table, String attr, String value) {
        try {
            table = URLEncoder.encode(table, "UTF-8");
            attr = URLEncoder.encode(attr, "UTF-8");
            value = URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return new StringBuilder(table).append("()").append(attr).append("()").append(value).append(".bitmap").toString();
    }


}
