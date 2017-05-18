package org.rcsb.exonscoassociation.tools;

import com.google.common.collect.Sets;
import org.rcsb.genevariation.datastructures.PeptideRange;
import scala.Tuple2;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by yana on 5/17/17.
 */
public class PeptideRangeTool {

    public static Tuple2<PeptideRange, PeptideRange> getExonsPairWithBestStructuralCoverage(List<PeptideRange> mapping1,
                                                                                            List<PeptideRange> mapping2) {

        Set<String> s1 = mapping1.stream().map(t -> t.getStructureId()).collect(Collectors.toSet());
        Set<String> s2 = mapping2.stream().map(t -> t.getStructureId()).collect(Collectors.toSet());
        Sets.SetView<String> intersection = Sets.intersection(s1, s2);

        int coverage=0;
        Tuple2<PeptideRange, PeptideRange> pair = null;
        for (String structureId : intersection) {

            PeptideRange r1 = mapping1.stream().filter(t -> t.getStructureId().equals(structureId))
                    .collect(Collectors.toList()).get(0);
            PeptideRange r2 = mapping2.stream().filter(t -> t.getStructureId().equals(structureId))
                    .collect(Collectors.toList()).get(0);

            if ( (r1.getStructureLength()+r2.getStructureLength()) > coverage ) {
                pair = new Tuple2<>(r1, r2);
                coverage = r1.getStructureLength()+r2.getStructureLength();
            }
            else if ( (r1.getStructureLength()+r2.getStructureLength()) == coverage ) {
                if ( r1.isExperimental() ) {
                    if (r1.getResolution() < pair._1.getResolution()) {
                        pair = new Tuple2<>(r1, r2);
                    }
                }
            }
        }
        return pair;
    }
}
