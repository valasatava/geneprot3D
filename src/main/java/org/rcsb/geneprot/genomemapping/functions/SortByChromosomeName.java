package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;

/**
 * Created by Yana Valasatava on 8/10/17.
 */
public class SortByChromosomeName implements Function<String, Integer>
{
    public Integer call(String s) throws Exception {
        if (s.startsWith("chr")) {
            s = s.substring(3);
        }

        if (s.startsWith("Chr")) {
            s = s.substring(3);
        }

        if (s.indexOf("_") > -1) {
            String[] spl = s.split("_");
            s = spl[0];
        }

        return s.equalsIgnoreCase("X") ? Integer.valueOf(99) :
                (s.equalsIgnoreCase("Y") ? Integer.valueOf(100) :
                        (s.equalsIgnoreCase("M") ? Integer.valueOf(101) : // Mitochondrial DNA
                                (s.equalsIgnoreCase("Un") ? Integer.valueOf(101) : Integer.valueOf(Integer.parseInt(s)))));
    }
}