package edu.caltech.nanodb.storage.bitmapfile;

import org.apache.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;

/**
 * A version of bitset implemented with the RoaringBitmap library. More info at https://github.com/lemire/RoaringBitmap
 */
public class BitSetRoaringImpl implements BitSet {
    private static Logger logger = Logger.getLogger(BitSetRoaringImpl.class);

    private RoaringBitmap bitset;

    public BitSetRoaringImpl() {
        this.bitset = new RoaringBitmap();
    }

    public void and(BitSet other) {
        this.bitset.and(((BitSetRoaringImpl)other).bitset);
    }

    public void or(BitSet other) {
        this.bitset.or(((BitSetRoaringImpl)other).bitset);
    }

    public void xor(BitSet other) {
        this.bitset.xor(((BitSetRoaringImpl)other).bitset);
    }

    public void andNot(BitSet other) {
        this.bitset.andNot(((BitSetRoaringImpl)other).bitset);
    }

    public void add(int x) {
        this.bitset.add(x);
    }

    public void remove(int x) {
        this.bitset.remove(x);
    }

    public int cardinality() {
        return this.bitset.getCardinality();
    }

    public boolean get(int x) {
        return this.bitset.contains(x);
    }

    public int size() {
        return this.bitset.serializedSizeInBytes();
    }

    public int rank(int x) {
        return this.bitset.rank(x);
    }

    public boolean isEmpty() {
        return this.bitset.isEmpty();
    }

    public int[] toArray() {
        return this.bitset.toArray();
    }

    public void load(byte[] input) {
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        DataInputStream dis = new DataInputStream(bais);
        try {
            this.bitset.deserialize(dis);
        } catch (IOException e) {
            logger.error("Failed to load byte array into bitset object!");
        }
    }

    public byte[] save() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            this.bitset.serialize(dos);
        } catch (IOException e) {
            logger.error("Failed to save bitset to a byte array!");
        }
        return baos.toByteArray();
    }

    public BitSet clone() {
        BitSetRoaringImpl ret = new BitSetRoaringImpl();
        ret.bitset = this.bitset.clone();
        return ret;
    }
}
