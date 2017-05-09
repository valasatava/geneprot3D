package org.rcsb.genevariation.dataframes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.rcsb.exonscoassociation.utils.RowUtils;
import org.rcsb.genevariation.datastructures.PeptidePosition;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 5/9/17.
 */
public class PeptidePositionMapper {

    private static final String DEFAULT_MAPPING_LOCATION = DataLocationProvider.getGencodeStructuralMappingLocation();
    private String MAPPING_LOCATION;

    public PeptidePositionMapper() {
        MAPPING_LOCATION = DEFAULT_MAPPING_LOCATION;
    }

    public PeptidePositionMapper(String LOCATION) {
        MAPPING_LOCATION = LOCATION;
    }

    public List<PeptidePosition> getChromosomeMappings(String chr) {
        return null;
    }

    public List<PeptidePosition> getExonMappings(String chr, int start, int end) {

        Dataset<Row> d = SaprkUtils.getSparkSession().read().parquet(MAPPING_LOCATION+"/"+chr)
                .filter(col("start").equalTo(start).and(col("end").equalTo(end)));
        d.show();
        List<Row> data = d.collectAsList();

        List<PeptidePosition> out = new ArrayList<PeptidePosition>();
        for (Row row : data ) {

            PeptidePosition pp = new PeptidePosition();

            pp.setChromosome(chr);
            pp.setEnsemblId(RowUtils.getEnsemblId(row));
            pp.setGeneBankId(RowUtils.getGeneBankId(row));
            pp.setGenomicCoordsStart(start);
            pp.setGenomicCoordsEnd(end);

            pp.setUniProtId(RowUtils.getUniProtId(row));
            pp.setUniProtCoordsStart(RowUtils.getUniProtStart(row));
            pp.setUniProtCoordsEnd(RowUtils.getUniProtEnd(row));

            pp.setExperimental(RowUtils.isPDBStructure(row));
            if (pp.isExperimental()) {
                pp.setStructureId(RowUtils.getPdbId(row)+"_"+RowUtils.getChainId(row));
            }
            else {
                pp.setStructureId(RowUtils.getTemplate(row));
            }
            pp.setStructCoordsStart(RowUtils.getStructStart(row));
            pp.setStructCoordsEnd(RowUtils.getStructEnd(row));

            out.add(pp);
        }
        return out;
    }

    public static void main(String[] args) {

        PeptidePositionMapper mapper = new PeptidePositionMapper();
        
        List<PeptidePosition> exon1 = mapper.getExonMappings("chr7", 65970282, 65970366);
        List<PeptidePosition> exon2 = mapper.getExonMappings("chr7", 65974919, 65975071);

    }
}
