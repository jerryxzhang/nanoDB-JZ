package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.*;
import edu.caltech.nanodb.storage.*;
import edu.caltech.nanodb.storage.heapfile.HeapTupleFileManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * A class that translates tuple indexes to tuple file locations. All reads and writes are
 * backed from disk.
 */
public class PointerList {
    private static Logger logger = Logger.getLogger(PointerList.class);

    private String filename;
    private StorageManager storageManager;
    private HeapTupleFileManager tupleFileManager;

    private TupleFile file;
    private ArrayList<FilePointer> pointers;

    public PointerList(String filename, StorageManager manager) {
        this.filename = filename;
        this.storageManager = manager;
        this.tupleFileManager = (HeapTupleFileManager) manager.getTupleFileManager(DBFileType.HEAP_TUPLE_FILE);
        pointers = new ArrayList<FilePointer>();
    }

    /**
     * Creates a new PointerList file and initializes it to be empty. Use setPointer to initialize the values.
     */
    public void createFile() {
        if (storageManager.getFileManager().fileExists(filename)) throw new IllegalArgumentException("Already Exists!");

        TableSchema schema = new TableSchema();
        ColumnType columnType = new ColumnType(SQLDataType.FILE_POINTER);
        ColumnInfo columnInfo = new ColumnInfo("pointer", filename, columnType);
        schema.addColumnInfo(columnInfo);

        try {
            DBFile dbFile = storageManager.createDBFile(filename, DBFileType.HEAP_TUPLE_FILE);
            file = tupleFileManager.createTupleFile(dbFile, schema);
        } catch (IOException e) {
            logger.error("Couldn't create file for PointerList " + filename);
        }
    }

    /**
     * Loads a PointerList file and adds all the pointers to this list.
     * TODO Changed this so we don't have to look through all the tuples since its ridiculous
     */
    public void loadFile() {
        try {
            DBFile dbFile = storageManager.openDBFile(filename);
            this.file = tupleFileManager.openTupleFile(dbFile);

            Tuple tuple = file.getFirstTuple();
            while (true) {
                FilePointer value = (FilePointer) tuple.getColumnValue(0);
                pointers.add(value);
                tuple = file.getNextTuple(tuple);
                if (tuple == null) break;
            }
        } catch (IOException e) {
            logger.error("Couldn't load PointerList " + filename);
        }
    }

    /**
     * Adds a pointer to this PointerList and makes the changes to disk as well
     */
    public void addPointer(FilePointer value) {
        this.pointers.add(value);
        try {
            this.file.addTuple(new TupleLiteral(value));
        } catch (IOException e) {
            logger.error("Couldn't add value to PointerList " + filename);
        }
    }

    /**
     * Modifies the pointer at the given index
     */
    public void setPointer(FilePointer value, int index) {
        // Have to calculate translation from index to pointer within this file to do this
        throw new UnsupportedOperationException("Not yet implemented");

/*        this.pointers.set(index, value);
        try {
            this.file.addTuple(new TupleLiteral(value));
        } catch (IOException e) {
            logger.error("Couldn't add value to PointerList " + filename);
        }*/
    }

    /**
     * Return all pointers in this list
     */
    public FilePointer getPointer(int index) {
        return pointers.get(index);
    }
}
