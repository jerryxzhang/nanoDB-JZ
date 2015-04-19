package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.relations.TableInfo;

import java.util.HashMap;

/**
 * This file represents a complete bitmap index as one would create it through
 * DDL. The index is created on one table and one value. The parts include one existence bitmap,
 * one locator bitmap, and many value bitmaps, one for each distinct value of the attribute.
 * The existence bitmap and locator bitmap are the same for all indexes on the same table, while
 * the value bitmaps are specific to this attribute.
 */
public class BitmapIndex {
    TableInfo table;
    String attribute;
    Bitmap locator;
    Bitmap existence;
    HashMap<String, Bitmap> valueBitmaps;

    //TODO A BitmapIndexManager that does some things such as keep track of existing indexes and reusing locator and
    // existence objects. In charge of creating indexes for a table. The index should do its own updating work and getting
    // stuff from existence and table and such

    public BitmapIndex(TableInfo table, String attribute) {
        this.table = table;
        this.attribute = attribute;
    }

    /**
     *
     */
    public void populate() {

    }

    public TableInfo getTableInfo() {
        return table;
    }

    public String getAttribute() {
        return attribute;
    }

    /**
     * Encode the table, attribute, and value for a bitmap into a single string.
     */
    public static String getBitmapIndexName(String table, String attr, String value) {
        return new StringBuilder("BITMAP__").append(table).append("__").append(attr).append("__").append(value).toString();
    }


}
