package org.rcsb.coassociated_exons.sandbox;

import com.google.common.collect.Range;

/**
 * Created by yana on 4/25/17.
 */
public class TestRangeIntersection {

    public static void foo() {

        Range<Integer> range1 = Range.closed(1, 9);
        Range<Integer> range2 = Range.closed(11, 19);
        Range<Integer> range3 = Range.closed(5, 15);

        if (range1.isConnected(range2)) {
            Range<Integer> intersection1 = range1.intersection(range2);
        }
        if (range1.isConnected(range3)) {
            Range<Integer> intersection2 = range1.intersection(range3);
            System.out.println(intersection2.toString());
        }

    }

    public static void main(String[] args) {
        foo();
    }
}
