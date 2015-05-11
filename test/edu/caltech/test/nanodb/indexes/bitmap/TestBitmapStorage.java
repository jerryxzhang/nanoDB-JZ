package edu.caltech.test.nanodb.indexes.bitmap;


import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.bitmapfile.*;
import edu.caltech.test.nanodb.sql.SqlTestCase;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Random;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * These tests test storage aspects of bitmap indexes i.e. making sure that all aspects of
 * bitmap indexes read and write correctly.
 */
@Test
public class TestBitmapStorage extends SqlTestCase {
    private static Logger logger = Logger.getLogger(TestBitmapStorage.class);

    public TestBitmapStorage() {
        super(null);
    }

    /**
     * This test creates a ValueSet, adds values to it, writes it to disk,
     * and checks if the value are intact.
     */
    public void testValueSet() throws Throwable {
        StorageManager storageManager = server.getStorageManager();
        String filename = "VALUESET";

        ValueSet set = new ValueSet(filename, storageManager);
        set.createFile();

        set.addValue("Value 1 fdsklahferoqgherpqgodfvnhaknvdq[ew");
        set.addValue("Value 2 gsqitqrppqtpqwetqwet");
        set.addValue("Value 3 rewqt4ui23t904u-f9234jfut0");
        set.addValue("Value 4 qqewqrghreoih34-g4389-2-4143");
        set.addValue("Value 5 32vt43720cthuoasfhdecqwohertumq0980");

        storageManager.getBufferManager().flushAll();
        assert(storageManager.getFileManager().fileExists(filename));

        ValueSet set2 = new ValueSet(filename, storageManager);
        set2.loadFile();

        assert(set.getValues().containsAll(set2.getValues()));
    }

    /**
     * This test attempts to create bitmaps of various sizes, store them on disk, and
     * read them back to check their integrity. Several caches are flushed to ensure the files
     * are actually written and read from disk.
    */
    public void testBitmapReadWrite() throws Throwable {
        StorageManager storageManager = server.getStorageManager();
        BitmapFileManager bitmapFileManager = new BitmapFileManager(storageManager);

        int mapsize = 10000;
        String filename = "TESTBITMAP";
        Bitmap map = bitmapFileManager.createBitmapFile(filename, null);

        ArrayList<Integer> ints = new ArrayList<Integer>();
        Random rand = new Random();
        for (int i = 0; i < mapsize; i++) {
            int r = rand.nextInt(100000);
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

        int[] map2ints = map2.toArray();
        for (int i = 0; i < map2ints.length; i++) {
            assert(ints.contains(map2ints[i]));
        }
    }


}
