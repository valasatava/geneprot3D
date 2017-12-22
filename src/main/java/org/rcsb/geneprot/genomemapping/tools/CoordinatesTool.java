package org.rcsb.geneprot.genomemapping.tools;

/**
 * Created by Yana Valasatava on 11/15/17.
 */
public class CoordinatesTool {

    public static boolean hasOverlap(int start1, int end1, int start2, int end2) {

        int c1 = Math.max(start1, start2);
        int c2 = Math.min(end1, end2);
        if ( c1 <= c2 )
            return true;
        else
            return false;
    }

    /***
     *
     * @param start1
     * @param end1
     * @param start2
     * @param end2
     * @return int[] with overlapped interval or [-1, -1] if there is no overlap
     */
    public static int[] getOverlap(int start1, int end1, int start2, int end2) {

        int c1 = Math.max(start1, start2);
        int c2 = Math.min(end1, end2);
        if ( c1 <= c2 )
            return new int[]{c1, c2};
        else
            return new int[]{-1, -1};
    }

    public static int adjustStartCoordinateBaseOne(int start1, int point1, int start2) {
        int d = (point1 - start1);
        return start2 + d;
    }

    public static int adjustEndCoordinateBaseOne(int end1, int point1, int end2) {
        int d = (end1 - point1);
        return end2 - d;
    }

    public static int adjustStartCoordinateBaseOneToThree(int start1, int point1, int start2) {
        int d = (point1 - start1)*3;
        return start2 + d;
    }

    public static int adjustEndCoordinateBaseOneToThree(int end1, int point1, int end2) {
        int d = (end1 - point1)*3;
        return end2 - d;
    }

    public static int adjustStartCoordinateBaseThreeToOne(int start1, int point1, int start2) {
        int d = (point1 - start1 + 1);
        return start2 + (int) Math.ceil(d / 3.0f) - 1;
    }

    public static int adjustEndCoordinateBaseThreeToOne(int end1, int point1, int end2) {
        int d = (end1 - point1);
        return end2 - (int) Math.floor(d / 3.0f);
    }
}