package edu.caltech.nanodb.storage.bitmapfile;

/**
 * An abstract bitset interface. Provides functions for boolean operations on two bitsets,
 * setting and unsetting bits, and loading and saving from byte arrays. This allows the actual
 * implementation to be compressed in different formats if necessary.
 */
public interface BitSet {

    public void and(BitSet other);

    public void or(BitSet other);

    public void xor(BitSet other);

    public void andNot(BitSet other);

    /**
     * Sets the bit at x to true
     */
    public void add(int x);

    /**
     * Sets the bit at x to false
     */
    public void remove(int x);

    public int cardinality();

    public boolean get(int x);

    /**
     * Returns the size of the bitset, which will be exactly the size returned when serialized
     */
    public int size();

    /**
     * Returns the number of set bits at smaller or equal indexes to x
     */
    public int rank(int x);

    public boolean isEmpty();

    /**
     * Returns an array of the indices of set bits, in sorted order
     */
    public int[] toArray();

    /**
     * Given the bytes of a bitset object, load the object into this bitset, overwriting
     * any previous contents.
     */
    public void load(byte[] input);

    /**
     * Save the current contents of the bitset to be loaded in the future.
     */
    public byte[] save();

    public BitSet clone();

}
