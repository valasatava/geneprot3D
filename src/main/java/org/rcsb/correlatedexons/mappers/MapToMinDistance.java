package org.rcsb.correlatedexons.mappers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.rcsb.correlatedexons.utils.RowUtils;
import org.rcsb.correlatedexons.utils.StructureUtils;
import org.rcsb.genevariation.mappers.UniprotToModelCoordinatesMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 4/24/17.
 */
public class MapToMinDistance implements Function<List<Row>, List<String>> {

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

                int start1 = -1; int end1 = -1;
                int start2 = -1; int end2 = -1;

                if (pdbFlag) {

                    start1 = RowUtils.getPdbStart(exon1);
                    end1 = RowUtils.getPdbEnd(exon1);

                    start2 = RowUtils.getPdbStart(exon2);
                    end2 = RowUtils.getPdbEnd(exon2);

                } else {

                    int uniStart1 = RowUtils.getUniProtStart(exon1);
                    start1 = mapper.getModelCoordinateByUniprotPosition(uniStart1);
                    int uniEnd1 = RowUtils.getUniProtEnd(exon1);
                    end1 = mapper.getModelCoordinateByUniprotPosition(uniEnd1);

                    int uniStart2 = RowUtils.getUniProtStart(exon2);
                    start2 = mapper.getModelCoordinateByUniprotPosition(uniStart2);
                    int uniEnd2 = RowUtils.getUniProtEnd(exon2);
                    end2 = mapper.getModelCoordinateByUniprotPosition(uniEnd2);
                }

                List<Atom> atoms1 = StructureUtils.getAtomsInRange(groups, start1, end1);
                List<Atom> atoms2 = StructureUtils.getAtomsInRange(groups, start2, end2);

                double mindist = StructureUtils.getMinDistance(atoms1, atoms2);
                String line = String.format("%s,%s,%s,%s,%s,%.2f\n", gene, RowUtils.getExon(exon1), RowUtils.getExon(exon2), uniProtId, structureId, mindist);
                if (! results.contains(line))
                    results.add(line);
            }
        }

        return results;

    }
}
