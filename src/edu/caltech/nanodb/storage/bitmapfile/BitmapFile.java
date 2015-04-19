package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.DBPage;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * A class representing a Bitmap stored in a file. The format stores the same first two bytes as a DBFile,
 * followed by an int representing the size of the bitset, then the rest of the bytes are the bytes of the
 * bitset, in whatever format the bitset class chooses to serialize itself to.
 */
public class BitmapFile {
    private static Logger logger = Logger.getLogger(BitmapFile.class);

    /** The manager for bitmap files */
    private BitmapFileManager bitmapFileManager;

    /** The file that stores the index. */
    private DBFile dbFile;

    /** The bitmap the file is associated with */
    private Bitmap bitmap;

    public BitmapFile(DBFile dbFile, Bitmap bitmap, BitmapFileManager manager) {
        this.bitmapFileManager = manager;
        this.dbFile = dbFile;
        this.bitmap = bitmap;
    }

    public DBFile getDbFile() {
        return dbFile;
    }

    /**
     * Opens the DBFile associated with this BitmapFile and reads its contents into
     * this bitmap. First reads in the bitmap size, and loads that many bytes into
     * the bitmap. The previous bitmap is overwritten.
     */
    public void readFileToBitmap() {
        // Open up each page of this file and read the data into a single byte array
        ByteArrayOutputStream bitmapbytes = new ByteArrayOutputStream();
        try {
            int bitmapsize = -1;
            for (int i = 0; i < dbFile.getNumPages(); i++) {
                DBPage page = bitmapFileManager.getStorageManager().loadDBPage(dbFile, i);
                byte[] data = page.getPageData();

                if (i == 0) {
                    // Read the file type and size from the first page
                    if (DBFileType.valueOf(page.readByte(0)) != DBFileType.BITMAP_INDEX_FILE)
                        throw new IllegalStateException("Not a bitmap file!");
                    bitmapsize = page.readInt(2);

                    // Check if the rest of the data is shorter than the bitmap. If so, write
                    // the bytes and subtract the amount written from the size. Otherwise,
                    // write the bytes and finish.
                    if (data.length - 6 < bitmapsize) {
                        bitmapbytes.write(data, 6, data.length - 6);
                        bitmapsize -= data.length - 6;
                    } else {
                        bitmapbytes.write(data, 6, bitmapsize);
                        bitmapsize -= bitmapsize;
                        break;
                    }
                } else {
                    // The whole page is data
                    if (data.length < bitmapsize) {
                        bitmapbytes.write(data, 0, data.length);
                        bitmapsize -= data.length;
                    } else {
                        bitmapbytes.write(data, 0, bitmapsize);
                        bitmapsize -= bitmapsize;
                        break;
                    }
                }
            }
            if (bitmapsize != 0) throw new IllegalStateException("Bitmap reading from file error");
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        bitmap.load(bitmapbytes.toByteArray());
    }

    /**
     * Saves the serialized bytes of the bitset to the DBFile. First saves the size of the bytes as
     * an int, then breaks the data up into pages and saves one page at a time. The data is only actually
     * written when the BufferManager flushes it, though.
     */
    public void writeBitmapToFile() {
        // TODO only write if the page changed?
        byte[] bytes = bitmap.save();
        try {
            // size of the file to be written
            int size = bytes.length + 6;
            int pagesize = dbFile.getPageSize();
            int numPages = dbFile.getNumPages();
            // Break up the data into pages, and save each page
            for (int i = 0; i < size / pagesize + 1; i++) {
                DBPage page = bitmapFileManager.getStorageManager().loadDBPage(dbFile, i, true);
                if (i == 0) {
                    page.writeInt(2, bytes.length);
                    // Skip the first 6 bytes when writing to page 0
                    byte[] buffer = Arrays.copyOfRange(bytes, 0, pagesize - 6);
                    page.write(6, buffer);
                } else if (i < size / pagesize) {
                    // Write a full page
                    byte[] buffer = Arrays.copyOfRange(bytes, i * pagesize - 6, (i + 1) * pagesize - 6);
                    page.write(0, buffer);
                } else if (i == size / pagesize && size % pagesize != 0) {
                    // Write the extra bytes to the last page
                    byte[] buffer = Arrays.copyOfRange(bytes, (size / pagesize) * pagesize - 6, bytes.length);
                    page.write(0, buffer);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
