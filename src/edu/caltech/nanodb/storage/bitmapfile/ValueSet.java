package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.*;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.heapfile.HeapTupleFileManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A class representing a set of values to be used with Bitmap Indexes. The values are
 * backed to disk as a tuple file.
 */
public class ValueSet {
    private static Logger logger = Logger.getLogger(ValueSet.class);
    private static final int MAX_VALUE_LENGTH = 65535;

    private String filename;
    private StorageManager storageManager;
    private HeapTupleFileManager tupleFileManager;

    private TupleFile file;
    private Set<String> values;

    public ValueSet(String filename, StorageManager manager) {
        this.filename = filename;
        this.storageManager = manager;
        this.tupleFileManager = (HeapTupleFileManager) manager.getTupleFileManager(DBFileType.HEAP_TUPLE_FILE);
        values = new HashSet<String>();
    }

    /**
     * Creates a new ValueSet file and initializes it to be empty. Use addValue to initialize the values.
     */
    public void createFile() {
        if (storageManager.getFileManager().fileExists(filename)) throw new IllegalArgumentException("Already Exists!");

        TableSchema schema = new TableSchema();
        ColumnType columnType = new ColumnType(SQLDataType.VARCHAR);
        columnType.setLength(MAX_VALUE_LENGTH);
        ColumnInfo columnInfo = new ColumnInfo("value", filename, columnType);
        schema.addColumnInfo(columnInfo);

        try {
            DBFile dbFile = storageManager.createDBFile(filename, DBFileType.HEAP_TUPLE_FILE);
            file = tupleFileManager.createTupleFile(dbFile, schema);
        } catch (IOException e) {
            logger.error("Couldn't create file for ValueSet " + filename);
        }
    }

    /**
     * Loads a ValueSet file and adds all the values to this set.
     */
    public void loadFile() {
        try {
            DBFile dbFile = storageManager.openDBFile(filename);
            this.file = tupleFileManager.openTupleFile(dbFile);

            Tuple tuple = file.getFirstTuple();
            while (true) {
                String value = (String) tuple.getColumnValue(0);
                values.add(value);

                tuple = file.getNextTuple(tuple);
                if (tuple == null) break;
            }
        } catch (IOException e) {
            logger.error("Couldn't load ValueSet " + filename);
        }
    }

    /**
     * Adds a value to this ValueSet and makes the changes to disk as well
     */
    public void addValue(String value) {
        this.values.add(value);
        try {
            this.file.addTuple(new TupleLiteral(value));
        } catch (IOException e) {
            logger.error("Couldn't add value to ValueSet " + filename);
        }
    }

    public void removeValue(String value) {
        values.remove(value);
        try {
            Tuple tuple = file.getFirstTuple();
            while (true) {
                if (((String) tuple.getColumnValue(0)).equals(value)) file.deleteTuple(tuple);

                tuple = file.getNextTuple(tuple);
                if (tuple == null) break;
            }
        } catch (IOException e) {
            logger.error("Couldn't load ValueSet " + filename);
        }
    }

    /**
     * Return all values in this set
     */
    public Set<String> getValues() {
        return Collections.unmodifiableSet(values);
    }
}
