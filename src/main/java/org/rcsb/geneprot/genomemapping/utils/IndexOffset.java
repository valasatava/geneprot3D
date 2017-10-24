package org.rcsb.geneprot.genomemapping.utils;

import java.util.Arrays;

/**
 * Created by ap3 on 07/05/2014.
 * <p/>
 * A class that tracks index offset when re-constructing UniProt Isoforms.
 *
 */
public class IndexOffset {


    private static final boolean debug = false;
    short[] offset;

    public IndexOffset(int length) {
        offset = getOffsetArray(length);
    }

    public short[] getOffsetArray(int length) {

        short[] data = new short[length];
        for (int i = 0; i < length; i++) {
            data[i] = 0;
        }
        return data;
    }

    public int getOffset(int position) {
        return position + offset[position];
    }

    public void setOffset(int start, int end, short modifier) {
        for (int i = start; i < end; i++) {
            offset[i] += modifier;
        }
    }

    /** Changes the length of the offset array
     *
     * @param length
     */
    public void adjustOffset(int length) {

        int pos = offset.length;
        offset = Arrays.copyOf(offset, length);

        if (pos < length) {
            for (int i = pos; i < length; i++) {
                offset[i] = 0;
            }
        }
    }

    public int getLength() {
        return offset.length;
    }
}
