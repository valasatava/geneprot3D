package org.rcsb.geneprot.genomemapping.tools;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class RangeConverter {

    public int s1;
    public int e1;
    public int s2;
    public int e2;

    public void set(int start1, int end1, int start2, int end2) {
        s1 = start1;
        e1 = end1;
        s2 = start2;
        e2 = end2;
    }

    public int[] convert1to2(int c1, int c2) {

        int start = -1;
        int end = -1;

        if (c2 < e1) {

            start = c1;
            if (c1 <= s1)
                start = s2;

            end = c2;
            if (c2 >= e1)
                start = e2;
        }
        return new int[]{start, end};
    }
}
