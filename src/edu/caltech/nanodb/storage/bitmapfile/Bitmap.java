package edu.caltech.nanodb.storage.bitmapfile;

import edu.caltech.nanodb.relations.TableInfo;

/**
 * Represents an individual bitmap of some type that can do operations against other bitmaps,
 * and be modified. Bitmaps can be disk-backed, in which case any operations modifying the bitmap
 * will cause its disk backing data to be modified as well. Bitmaps can be temporary, i.e. the
 * result of an operation on two bitmaps. These bitmaps will not cause any disk change when
 * modified.
 */
public class Bitmap {

    private String name;
    private BitmapFile bitmapFile;
    private BitSet bitset;
    private BitmapIndex parent;

    public Bitmap(BitmapIndex parent, String name) {
        this.parent = parent;
        this.name = name;
        this.bitset = null;
        this.bitmapFile = null;
    }

    public void setBitmapFile(BitmapFile bitmapFile) {
        this.bitmapFile = bitmapFile;
    }

    public void setBitSet(BitSet bitset) {
        this.bitset = bitset;
    }

    public boolean isDiskBacked() {
        return bitmapFile != null;
    }

    public TableInfo getTableInfo() {
        return parent.getTableInfo();
    }

    public String getTableName() {
        return parent.getTableInfo().getTableName();
    }

    public String getAttribute() {
        return parent.getAttribute();
    }

    public String getName() {
        return name;
    }

    public void load(byte[] bytes) {
        this.bitset.load(bytes);
    }

    public byte[] save() {
        return this.bitset.save();
    }

    public Bitmap clone() {
        Bitmap ret = new Bitmap(parent, null);
        ret.setBitSet(bitset.clone());
        return ret;
    }

    public int[] toArray() {
        return this.bitset.toArray();
    }

    public int cardinality() {
        return this.bitset.cardinality();
    }

    /**
     * Set a bit as the given position. If this is a disk backed bitmap, make the same change to
     * the file by writing the bitmap to file.
     */
    public void set(int pos) {
        this.bitset.add(pos);
        if (isDiskBacked())
            this.bitmapFile.writeBitmapToFile();
    }

    /**
     * Unset a bit as the given position. If this is a disk backed bitmap, make the same change to
     * the file by writing the bitmap to file.
     */
    public void unset(int pos) {
        this.bitset.remove(pos);
        if (isDiskBacked())
            this.bitmapFile.writeBitmapToFile();
    }

    public boolean contains(int pos) {
        return this.bitset.get(pos);
    }

    /**
     * Returns the and of two bitmaps as a new bitmap. The new bitmap will have the same
     * index parent, but will not have a name or file associated with it. The inputs are
     * not modified.
     */
    public static Bitmap and(Bitmap m1, Bitmap m2) {
        if (m1.parent != m2.parent)
            throw new IllegalArgumentException("Can't operate on bitmaps from different indexes");
        Bitmap ret = m1.clone();
        ret.bitset.and(m2.bitset);
        return ret;
    }

    /**
     * Returns the or of two bitmaps as a new bitmap. The new bitmap will have the same
     * index parent, but will not have a name or file associated with it. The inputs are
     * not modified.
     */
    public static Bitmap or(Bitmap m1, Bitmap m2) {
        if (m1.parent != m2.parent)
            throw new IllegalArgumentException("Can't operate on bitmaps from different indexes");
        Bitmap ret = m1.clone();
        ret.bitset.or(m2.bitset);
        return ret;
    }
}
