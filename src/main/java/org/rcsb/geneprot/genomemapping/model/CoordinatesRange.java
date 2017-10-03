package org.rcsb.geneprot.genomemapping.model;

import java.io.Serializable;

/**
 * Created by Yana Valasatava on 10/2/17.
 */
public class CoordinatesRange implements Serializable {

    private int start;
    private int end;

    public CoordinatesRange() {}

    public CoordinatesRange(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}
