package edu.caltech.test.nanodb.indexes.bitmap;


import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.bitmapfile.Bitmap;
import edu.caltech.nanodb.storage.bitmapfile.BitmapFileManager;
import edu.caltech.test.nanodb.sql.SqlTestCase;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Random;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by j on 4/18/2015.
 */
@Test
public class TestBitmapStorage extends SqlTestCase {
    private static Logger logger = Logger.getLogger(TestBitmapStorage.class);

    public TestBitmapStorage() {
        super(null);
    }

    /**
     * This test attempts to create bitmaps of various sizes, store them on disk, and
     * read them back to check their integrity. Several caches are flushed to ensure the files
     * are actually written and read from disk.
    */
    public void testBitmapReadWrite() throws Throwable {
        StorageManager storageManager = server.getStorageManager();
        BitmapFileManager bitmapFileManager = new BitmapFileManager(storageManager);

        int mapsize = 10;
        String filename = "TESTBITMAP";
        Bitmap map = bitmapFileManager.createBitmapFile(filename, null);

        ArrayList<Integer> ints = new ArrayList<Integer>();
        Random rand = new Random();
        for (int i = 0; i < mapsize; i++) {
            int r = rand.nextInt(10000);
            if (!ints.contains(r)) {
                ints.add(r);
                map.set(r);
            }
        }
        Collections.sort(ints);

        bitmapFileManager.clearCache();
        storageManager.getBufferManager().flushAll();
        assert(storageManager.getFileManager().fileExists(filename));

        Bitmap map2 = bitmapFileManager.loadBitmapFile(filename, null);

        logger.info(Arrays.toString(map2.save()));
        logger.info(Arrays.toString(map2.toArray()));
        logger.info(ints);

        for (int i = 0; i < map2.toArray().length; i++) {
            assert(ints.contains(map2.toArray()[i]));
        }
    }
}
