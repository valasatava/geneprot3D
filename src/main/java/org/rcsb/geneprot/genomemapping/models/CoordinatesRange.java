package org.rcsb.geneprot.genomemapping.models;

import java.io.Serializable;

/**
 * Created by Yana Valasatava on 10/2/17.
 */
public class CoordinatesRange implements Serializable {

    private int id;
    private int start;
    private int end;

    public CoordinatesRange() {}

    public CoordinatesRange(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public CoordinatesRange(int id, int start, int end) {
        this.id = id;
        this.start = start;
        this.end = end;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
