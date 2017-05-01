package org.rcsb.coassociated_exons.sandbox;

/**
 * Created by yana on 4/18/17.
 */
public class FromTo {

    private int from;
    private int to;

    public FromTo(int from, int to) {
        this.from = from;
        this.to = to;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getTo() {
        return to;
    }

    public void setTo(int to) {
        this.to = to;
    }
}
