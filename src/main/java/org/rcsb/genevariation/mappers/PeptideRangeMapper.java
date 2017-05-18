package org.rcsb.genevariation.mappers;

import com.google.common.collect.Range;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.rcsb.exonscoassociation.tools.PeptideRangeTool;
import org.rcsb.exonscoassociation.utils.RowUtils;
import org.rcsb.exonscoassociation.utils.StructureUtils;
import org.rcsb.genevariation.datastructures.PeptideRange;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 5/9/17.
 */
public class PeptideRangeMapper {

    private static final String DEFAULT_MAPPING_LOCATION = DataLocationProvider.getGencodeStructuralMappingLocation();
    private String MAPPING_LOCATION;

    public PeptideRangeMapper() {
        MAPPING_LOCATION = DEFAULT_MAPPING_LOCATION;
    }

    public PeptideRangeMapper(String LOCATION) {
        MAPPING_LOCATION = LOCATION;
    }

    public List<PeptideRange> getMappingForRange(String chr, int start, int end) throws Exception {

        Dataset<Row> chrom = SaprkUtils.getSparkSession().read().parquet(MAPPING_LOCATION + "/" + chr);
        return getMappingForRange(chrom, start, end);
    }

    public List<PeptideRange> getMappingForRange(Dataset<Row> chrom, int start, int end) throws Exception {

        Dataset<Row> data = chrom.filter(col("start").equalTo(start).and(col("end").equalTo(end)));
        List<Row> dataList = data.collectAsList();

        List<PeptideRange> mapping = new ArrayList<>();
        UniprotToModelCoordinatesMapper mapper = new UniprotToModelCoordinatesMapper();

        for (Row row : dataList ) {

            PeptideRange pp = new PeptideRange();

            pp.setChromosome(RowUtils.getChromosome(row));
            pp.setEnsemblId(RowUtils.getEnsemblId(row));
            pp.setGeneBankId(RowUtils.getGeneBankId(row));
            pp.setGenomicCoordsStart(start);
            pp.setGenomicCoordsEnd(end);

            pp.setUniProtId(RowUtils.getUniProtId(row));
            pp.setUniProtCoordsStart(RowUtils.getUniProtStart(row));
            pp.setUniProtCoordsEnd(RowUtils.getUniProtEnd(row));

            pp.setExperimental(RowUtils.isPDBStructure(row));

            List<Group> groups;
            Range<Integer> range;

            if (pp.isExperimental()) {

                pp.setStructureId(RowUtils.getPdbId(row)+"_"+RowUtils.getChainId(row));
                range = RowUtils.getPdbRange(row);

                Structure structure = StructureUtils.getBioJavaStructure(RowUtils.getPdbId(row));
                float resolution = structure.getPDBHeader().getResolution();
                pp.setResolution(resolution);

                if ( range.lowerEndpoint()==Integer.MIN_VALUE
                        || range.upperEndpoint()==Integer.MAX_VALUE ) {

                    Chain chain = structure.getPolyChainByPDB(RowUtils.getChainId(row));
                    groups = chain.getAtomGroups();

                    List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups, range.lowerEndpoint(),
                            range.upperEndpoint());
                    pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
                    pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size()-1).getResidueNumber().getSeqNum());

                } else {
                    pp.setStructuralCoordsStart(range.lowerEndpoint());
                    pp.setStructuralCoordsEnd(range.upperEndpoint());
                }

            } else {

                pp.setStructureId(RowUtils.getTemplate(row));
                RowUtils.setUTMmapperFromRow(mapper, row);
                range = RowUtils.getModelRange(mapper, row);
                groups = StructureUtils.getGroupsFromModel(mapper.getCoordinates());

                List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups, range.lowerEndpoint(), range.upperEndpoint());
                if (groupsInRange.size()!=0) {
                    pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
                    pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size()-1).getResidueNumber().getSeqNum());
                }
            }
            mapping.add(pp);
        }
        return mapping;
    }

    public static void main(String[] args) throws Exception {

        PeptideRangeMapper mapper = new PeptideRangeMapper();

        Dataset<Row> chrom = SaprkUtils.getSparkSession().read()
                .parquet(mapper.MAPPING_LOCATION + "/" + "chr7").distinct().cache();

        List<PeptideRange> mapping1 = mapper.getMappingForRange(chrom, 65970282, 65970366);
        List<PeptideRange> mapping2 = mapper.getMappingForRange(chrom, 65974919, 65975071);

        Tuple2<PeptideRange, PeptideRange> pair = PeptideRangeTool.getExonsPairWithBestStructuralCoverage(mapping1, mapping2);

        System.out.println();

    }
}