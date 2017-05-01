package org.rcsb.coassociated_exons.sandbox;

/**
 * Created by yana on 4/17/17.
 */
public class BinTwo {
    public BinTwo(int id, int end) {
        this.id = id;
        this.end = end;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    private int id;
    private int end;
}
