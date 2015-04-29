package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFile;

import java.io.IOException;

/**
 * Created by j on 4/25/2015.
 */
public class BitmapIndexManager {
    private BitmapFileManager bitmapFileManager;

    public BitmapIndexManager(StorageManager storageManager) {
        this.bitmapFileManager = new BitmapFileManager(storageManager);
    }

    public BitmapFileManager getBitmapFileManager() {
        return bitmapFileManager;
    }

    public BitmapIndex createBitmapIndex(TableInfo tableInfo, String attribute) {
        BitmapIndex bitmapIndex = null;
        try {
            bitmapIndex = new BitmapIndex(tableInfo, attribute, this);

            bitmapIndex.populate();

            //TODO indicate in table schema that index exists

        } catch (IOException e) {

        }
        return bitmapIndex;
    }

    public void dropBitmapIndex(TableInfo tableInfo, String attribute) {

    }

    public void openBitmapIndex(TableInfo tableInfo, String attribute) {

    }
}
