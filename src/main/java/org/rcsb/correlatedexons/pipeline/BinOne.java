package org.rcsb.correlatedexons.pipeline;

/**
 * Created by yana on 4/17/17.
 */
public class BinOne {
    public BinOne(int id, int start) {
        this.id = id;
        this.start = start;
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

    private int id;
    private int start;
}
