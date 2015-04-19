package edu.caltech.nanodb.storage.bitmapfile;

import java.io.IOException;
import java.util.LinkedHashMap;

import edu.caltech.nanodb.storage.*;
import org.apache.log4j.Logger;


/**
 * This class provides high-level operations on bitmaps such as creating a new bitmap
 * or loading a bitmap from a file. Bitmaps are cached for easy reuse.
 */
public class BitmapFileManager {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BitmapFileManager.class);

    /** Maximum number of bitmaps that can be stored in the cache **/
    private static final int MAX_CACHE = 10;

    /** A reference to the storage manager. */
    private StorageManager storageManager;

    // TODO add cache eviction in case the cache gets too full.
    // if cache gets bigger than 10, evict the oldest one. maybe change to be by size?
    /** A cache of the currently open indexes for easy retrieval **/
    private LinkedHashMap<String, Bitmap> openBitmaps;

    public BitmapFileManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        openBitmaps = new LinkedHashMap<String, Bitmap>();
        this.storageManager = storageManager;
    }

    /**
     * Given a bitmap specification without a backing file or bitset, creates the bitmap file on disk
     * and allocates an empty bitset.
     */
    public Bitmap createBitmapFile(String idxFileName, BitmapIndex parent) {
        logger.info("Creating bitmap file for file " + idxFileName);

        Bitmap bitmap = new Bitmap(parent, idxFileName);

        FileManager fileManager = storageManager.getFileManager();

        if (openBitmaps.containsKey(idxFileName) || fileManager.fileExists(idxFileName)) {
            throw new IllegalArgumentException("Index file already exists for this " + idxFileName);
        }

        int pageSize = StorageManager.getCurrentPageSize();
        DBFileType type = DBFileType.BITMAP_INDEX_FILE;


        // First, create a new rile that the index file will go into.
        DBFile dbFile = null;
        try {
            dbFile = fileManager.createDBFile(idxFileName, type, pageSize);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        BitmapFile bitmapFile = new BitmapFile(dbFile, bitmap, this);

        // Create a new bitmap
        bitmap.setBitmapFile(bitmapFile);
        BitSet bitset = new BitSetRoaringImpl();
        bitmap.setBitSet(bitset);

        // Write the empty bitmap to file
        bitmapFile.writeBitmapToFile();

        // Cache this index since it's now considered "open".
        openBitmaps.put(idxFileName, bitmap);

        return bitmap;
    }

    /**
     * Given info on the bitmap file, load the bitmap from file, or from cache if it is in the cache.
     */
    public Bitmap loadBitmapFile(String idxFileName, BitmapIndex parent) {

        // Get it from the cache if possible
        if (openBitmaps.containsKey(idxFileName)) return openBitmaps.get(idxFileName);

        if (!storageManager.getFileManager().fileExists(idxFileName))
            throw new IllegalArgumentException("Doesn't exist! " + idxFileName);

        DBFile dbFile = null;
        try {
            dbFile = storageManager.openDBFile(idxFileName);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        logger.info("Opening existing bitmap index file " + idxFileName);

        if (dbFile.getType() != DBFileType.BITMAP_INDEX_FILE)
            throw new IllegalArgumentException("Not a bitmap index file");

        Bitmap bitmap = new Bitmap(parent, idxFileName);
        BitmapFile bitmapFile = new BitmapFile(dbFile, bitmap, this);
        bitmap.setBitmapFile(bitmapFile);

        BitSet bitset = new BitSetRoaringImpl();
        bitmap.setBitSet(bitset);
        bitmapFile.readFileToBitmap();

        // Cache this index since it's now considered "open".
        openBitmaps.put(idxFileName, bitmap);

        return bitmap;
    }

    public void clearCache() {
        this.openBitmaps.clear();
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }

}
