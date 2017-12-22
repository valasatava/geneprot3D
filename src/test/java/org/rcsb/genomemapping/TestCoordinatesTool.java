package org.rcsb.genomemapping;

import org.junit.Test;
import org.rcsb.geneprot.genomemapping.tools.CoordinatesTool;

import static org.junit.Assert.*;

/**
 * Created by Yana Valasatava on 10/26/17.
 */
public class TestCoordinatesTool {

    @Test
    public void testHasOverlapTrue() {

        int start1 = 1;
        int end1 = 5;
        int start2 = 3;
        int end2 = 7;

        assertTrue(CoordinatesTool.hasOverlap(start1, end1, start2, end2));
    }

    @Test
    public void testHasOverlapFalse() {

        int start1 = 1;
        int end1 = 5;
        int start2 = 8;
        int end2 = 10;

        assertFalse(CoordinatesTool.hasOverlap(start1, end1, start2, end2));
    }

    @Test
    public void testGetOverlapFullInclusiveInside() {

        int start1 = 1;
        int end1 = 10;
        int start2 = 3;
        int end2 = 7;

        int[] overlap = new int[]{3, 7};
        assertArrayEquals(CoordinatesTool.getOverlap(start1, end1, start2, end2), overlap);
    }

    @Test
    public void testGetOverlapFullInclusiveOutside() {

        int start1 = 5;
        int end1 = 10;
        int start2 = 3;
        int end2 = 15;

        int[] overlap = new int[]{5, 10};
        assertArrayEquals(CoordinatesTool.getOverlap(start1, end1, start2, end2), overlap);
    }

    @Test
    public void testGetOverlapPartial() {

        int start1 = 5;
        int end1 = 10;
        int start2 = 7;
        int end2 = 15;

        int[] overlap = new int[]{7, 10};
        assertArrayEquals(CoordinatesTool.getOverlap(start1, end1, start2, end2), overlap);
    }

    @Test
    public void testGetNoOverlap() {

        int start1 = 5;
        int end1 = 10;
        int start2 = 11;
        int end2 = 15;

        int[] overlap = new int[]{-1, -1};
        assertArrayEquals(CoordinatesTool.getOverlap(start1, end1, start2, end2), overlap);
    }

    @Test
    public void testAdjustStartCoordinateBaseOne() {

        int start1 = 1;
        int point1 = 5;
        int start2 = 3;

        assertEquals(CoordinatesTool.adjustStartCoordinateBaseOne(start1, point1, start2), 7);
    }

    @Test
    public void testAdjustEndCoordinateBaseOne() {

        int end1 = 10;
        int point1 = 5;
        int end2 = 13;

        assertEquals(CoordinatesTool.adjustEndCoordinateBaseOne(end1, point1, end2), 8);
    }

    @Test
    public void testAdjustStartCoordinateBaseOneToThree() {

        int start1 = 1;
        int point1 = 3;
        int start2 = 1;

        assertEquals(CoordinatesTool.adjustStartCoordinateBaseOneToThree(start1, point1, start2), 7);
    }

    @Test
    public void testAdjustEndCoordinateBaseOneToThreeAny() {

        int end1 = 4;
        int point1 = 2;
        int end2 = 12;

        assertEquals(CoordinatesTool.adjustEndCoordinateBaseOneToThree(end1, point1, end2), 6);
    }

    @Test
    public void testAdjustEndCoordinateBaseOneToThreeFirst() {

        int end1 = 3;
        int point1 = 1;
        int end2 = 9;

        assertEquals(CoordinatesTool.adjustEndCoordinateBaseOneToThree(end1, point1, end2), 3);
    }

    @Test
    public void testAdjustStartCoordinateBaseThreeToOneFirst() {

        int start1 = 1;
        int point1 = 4;
        int start2 = 1;

        assertEquals(CoordinatesTool.adjustStartCoordinateBaseThreeToOne(start1, point1, start2), 2);
    }

    @Test
    public void testAdjustStartCoordinateBaseThreeToOneMiddle() {

        int start1 = 1;
        int point1 = 5;
        int start2 = 1;

        assertEquals(CoordinatesTool.adjustStartCoordinateBaseThreeToOne(start1, point1, start2), 2);
    }

    @Test
    public void testAdjustStartCoordinateBaseThreeToOneLast() {

        int start1 = 1;
        int point1 = 6;
        int start2 = 1;

        assertEquals(CoordinatesTool.adjustStartCoordinateBaseThreeToOne(start1, point1, start2), 2);
    }

    @Test
    public void testAdjustEndCoordinateBaseThreeToOneFirst() {

        int end1 = 9;
        int point1 = 4;
        int end2 = 3;

        assertEquals(CoordinatesTool.adjustEndCoordinateBaseThreeToOne(end1, point1, end2), 2);
    }

    @Test
    public void testAdjustEndCoordinateBaseThreeToOneMiddle() {

        int end1 = 9;
        int point1 = 5;
        int end2 = 3;

        assertEquals(CoordinatesTool.adjustEndCoordinateBaseThreeToOne(end1, point1, end2), 2);
    }

    @Test
    public void testAdjustEndCoordinateBaseThreeToOneLast() {

        int end1 = 9;
        int point1 = 6;
        int end2 = 3;

        assertEquals(CoordinatesTool.adjustEndCoordinateBaseThreeToOne(end1, point1, end2), 2);

    }
}