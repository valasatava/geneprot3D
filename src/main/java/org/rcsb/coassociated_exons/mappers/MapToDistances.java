package org.rcsb.coassociated_exons.mappers;

import com.google.common.collect.Range;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.*;
import org.rcsb.coassociated_exons.utils.RowUtils;
import org.rcsb.coassociated_exons.utils.StructureUtils;
import org.rcsb.genevariation.mappers.UniprotToModelCoordinatesMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 4/24/17.
 */
public class MapToDistances implements Function<List<Row>, List<String>> {

    @Override
    public List<String> call(List<Row> transcript) throws Exception {

        List<String> results = new ArrayList<>();

        String gene = transcript.get(0).getString(0) + "," +
                transcript.get(0).getString(1) + "," +
                transcript.get(0).getString(2);

        String uniProtId = RowUtils.getUniProtId(transcript.get(0));
        String structureId = RowUtils.getPdbId(transcript.get(0))+"_"+RowUtils.getChainId(transcript.get(0));

        boolean pdbFlag = true;
        UniprotToModelCoordinatesMapper mapper = null;
        if (!RowUtils.isPDBStructure(transcript.get(0))) {
            pdbFlag = false;
            mapper = new UniprotToModelCoordinatesMapper(uniProtId);

            String url = RowUtils.getCoordinates(transcript.get(0));
            mapper.setTemplate(url);
            try {
                mapper.map();
            } catch (Exception e) {
                return results;
            }
            structureId = RowUtils.getTemplate(transcript.get(0));
        }

        Structure structure;
        Chain chain;
        if (pdbFlag) {
            structure = StructureUtils.getBioJavaStructure(RowUtils.getPdbId(transcript.get(0)));
            chain = structure.getPolyChainByPDB(RowUtils.getChainId(transcript.get(0)));
        } else {
            structure = StructureUtils.getModelStructure(RowUtils.getModelCoordinates(transcript.get(0)));
            chain = structure.getChainByIndex(0);
        }

        List<Group> groups = chain.getAtomGroups();

        int n = transcript.size();
        for (int i = 0; i < n - 1; i++) {
            for (int j = i + 1; j < n; j++) {

                Row exon1 = transcript.get(i);
                Row exon2 = transcript.get(j);

                if (RowUtils.getExon(exon1).equals("46189273_46189357_1") || RowUtils.getExon(exon2).equals("46189273_46189357_1")){
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
                    System.out.println(String.format("%s,%s,%s,%s", gene, uniProtId, structureId, RowUtils.getExon(exon1)));
                    continue;
                }
                List<Atom> atoms1 = StructureUtils.getAtomsInRange(range1);

                List<Group> range2 = StructureUtils.getGroupsInRange(groups,
                        pdbRange2.lowerEndpoint(), pdbRange2.upperEndpoint());
                if (range2.size()==0) {
                    System.out.println(String.format("%s,%s,%s,%s", gene, uniProtId, structureId, RowUtils.getExon(exon2)));
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