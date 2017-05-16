package org.rcsb.exonscoassociation.mappers;

import com.google.common.collect.Range;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.*;
import org.rcsb.exonscoassociation.utils.RowUtils;
import org.rcsb.exonscoassociation.utils.StructureUtils;
import org.rcsb.genevariation.mappers.UniprotToModelCoordinatesMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 4/24/17.
 */
public class MapToDistances implements Function<List<Row>, List<String>> {

    private static UniprotToModelCoordinatesMapper mapper = null;

    @Override
    public List<String> call(List<Row> transcript) throws Exception {

        List<String> results = new ArrayList<>();

        Row row = transcript.get(0);

        String gene = row.getString(0) + "," + row.getString(1) + "," + row.getString(2);

        String uniProtId = RowUtils.getUniProtId(row);
        String structureId;

        boolean pdbFlag = true;
        List<Group> groups;

        if (RowUtils.isPDBStructure(row)) {
            structureId = RowUtils.getPdbId(transcript.get(0))+"_"+RowUtils.getChainId(row);
            groups = StructureUtils.getGroupsFromPDBStructure(RowUtils.getPdbId(row), RowUtils.getPdbId(row));
        }

        else {

            pdbFlag = false;

            mapper = new UniprotToModelCoordinatesMapper();
            RowUtils.setUTMmapperFromRow(mapper, row);
            try {
                mapper.map();
            } catch (Exception e) {
                return results;
            }
            structureId = RowUtils.getTemplate(row);
            groups = StructureUtils.getGroupsFromModel(mapper.getCoordinates());
        }

        int n = transcript.size();
        for (int i = 0; i < n - 1; i++) {
            for (int j = i + 1; j < n; j++) {

                Row exon1 = transcript.get(i);
                Row exon2 = transcript.get(j);

                if (RowUtils.getExon(exon1).equals(RowUtils.getExon(exon2)))
                    continue;

                if (RowUtils.getExon(exon1).equals("204954829_204954951_2") && RowUtils.getExon(exon2).equals("204962108_204962158_2")) {
                    System.out.println();
                }

                int uniStart1 = RowUtils.getUniProtStart(exon1);
                int uniEnd1 = RowUtils.getUniProtEnd(exon1);

                int uniStart2 = RowUtils.getUniProtStart(exon2);
                int uniEnd2 = RowUtils.getUniProtEnd(exon2);

                // distance in sequence space
                int aaDist = uniStart2 - uniEnd1;
                if ( aaDist < 0 ) {
                    aaDist = uniStart1 - uniEnd2;
                }

                Range<Integer> pdbRange1, pdbRange2;
                if (pdbFlag) {
                    pdbRange1 = RowUtils.getPdbRange(exon1);
                    pdbRange2 = RowUtils.getPdbRange(exon2);

                } else {
                    pdbRange1 = RowUtils.getModelRange(mapper, exon1);
                    pdbRange2 = RowUtils.getModelRange(mapper, exon2);
                }

                List<Group> range1 = StructureUtils.getGroupsInRange(groups,
                        pdbRange1.lowerEndpoint(), pdbRange1.upperEndpoint());

                if (range1.size()==0) {
                    System.out.println("exon 1: "+String.format("%s,%s,%s,%s,%d,%d", gene, uniProtId, structureId, RowUtils.getExon(exon1), pdbRange1.lowerEndpoint().intValue(), pdbRange1.upperEndpoint().intValue()));
                    continue;
                }
                List<Atom> atoms1 = StructureUtils.getAtomsInRange(range1);

                List<Group> range2 = StructureUtils.getGroupsInRange(groups,
                        pdbRange2.lowerEndpoint(), pdbRange2.upperEndpoint());
                if (range2.size()==0) {
                    System.out.println("exon 2: "+String.format("%s,%s,%s,%s,%d,%d", gene, uniProtId, structureId, RowUtils.getExon(exon2), pdbRange2.lowerEndpoint().intValue(), pdbRange2.upperEndpoint().intValue()));
                    continue;
                }
                List<Atom> atoms2 = StructureUtils.getAtomsInRange(range2);

                // flags that indicate full or partial structural coverage
                float cov1 = range1.size()/((float)(uniEnd1-uniStart1+1));
                float cov2 = range2.size()/((float)(uniEnd2-uniStart2+1));

                // minimal distance between any pair of atoms
                double mindist = StructureUtils.getMinDistance(atoms1, atoms2);

                List<Atom> aa11 = StructureUtils.getFirstResidue(atoms1);
                List<Atom> aa1n = StructureUtils.getLastResidue(atoms1);

                List<Atom> aa21 = StructureUtils.getFirstResidue(atoms2);
                List<Atom> aa2n = StructureUtils.getLastResidue(atoms2);

                // get distances start/end
                double dS1S2 = StructureUtils.getMinDistance(aa11, aa21);
                double dE1E2 = StructureUtils.getMinDistance(aa1n, aa2n);
                double dS1E2 = StructureUtils.getMinDistance(aa11, aa2n);
                double dE1S2 = StructureUtils.getMinDistance(aa1n, aa21);

                String line = String.format("%s,%s,%s,%s,%.2f,%s,%.2f,%d,%.2f,%.2f,%.2f,%.2f,%.2f\n", gene, uniProtId, structureId, RowUtils.getExon(exon1), cov1, RowUtils.getExon(exon2), cov2, aaDist, mindist, dS1S2, dE1E2, dS1E2, dE1S2);
                if (! results.contains(line)) {
                    results.add(line);
                }
            }
        }
        return results;
    }
}