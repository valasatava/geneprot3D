package org.rcsb.geneprot.common.mappers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.datastructures.PeptideRange;
import org.rcsb.geneprot.common.utils.SaprkUtils;
import org.rcsb.geneprot.common.tools.PeptideRangeTool;
import org.rcsb.geneprot.transcriptomics.analysis.NGLScriptsGeneration;
import org.rcsb.geneprot.transcriptomics.utils.RowUtils;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Created by Yana Valasatava on 5/9/17.
 */
public class PeptideRangeMapper {

    private static final Logger logger = LoggerFactory.getLogger(PeptideRangeMapper.class);

    private static final String DEFAULT_MAPPING_LOCATION = DataLocationProvider.getGencodeStructuralMappingLocation();
    
    private String MAPPING_LOCATION;
    private static UniprotToModelCoordinatesMapper mapper;

    public PeptideRangeMapper() {
        MAPPING_LOCATION = DEFAULT_MAPPING_LOCATION;
        mapper = new UniprotToModelCoordinatesMapper();
    }

    public PeptideRangeMapper(String LOCATION) {
        MAPPING_LOCATION = LOCATION;
        mapper = new UniprotToModelCoordinatesMapper();
    }

    public String getMappingLocation() {
        return MAPPING_LOCATION;
    }

    /** Maps the genetic coordinates to PeptideRange data structure.
     *
     * @param chr the chromosome name
     * @param start the start genetic coordinate
     * @param end the end genetic coordinate
     *
     * @return the List of PeptideRange objects holding the structural mapping for the given
     *         genetic coordinates
     */
    public List<PeptideRange> mapGeneticCoordinatesToPeptideRange(String chr, int start, int end) throws Exception {
        Dataset<Row> chromosome = SaprkUtils.getSparkSession()
                .read().parquet(MAPPING_LOCATION + "/" + chr).cache();
        return mapGeneticCoordinatesToPeptideRange(chromosome, start, end);
    }

    /** Maps the genetic coordinates to PeptideRange data structure.
     *
     * @param chromosome Dataset<Row> dataframe with structural mapping for a chromosome
     *                   containing the genetic coordinates
     * @param start the start genetic coordinate
     * @param end the end genetic coordinate
     *
     * @return the List of PeptideRange objects holding the structural mapping for the given
     *         genetic coordinates
     */
    public List<PeptideRange> mapGeneticCoordinatesToPeptideRange(Dataset<Row> chromosome, int start, int end) {

        List<PeptideRange> mapping = new ArrayList<>();
        Dataset<Row> data = chromosome.filter(col("start").equalTo(start).and(col("end").equalTo(end)));
        List<Row> dataList = data.collectAsList();

        for (Row row : dataList ) {

            PeptideRange pp = new PeptideRange();

            // gene IDs
            pp.setEnsemblId(RowUtils.getEnsemblId(row));
            pp.setGeneBankId(RowUtils.getGeneBankId(row));
            pp.setGeneName(RowUtils.getGeneName(row));

            // genetic coordinates
            pp.setChromosome(RowUtils.getChromosome(row));
            pp.setGenomicCoordsStart(start);
            pp.setGenomicCoordsEnd(end);

            // UniProt coordinates
            pp.setUniProtId(RowUtils.getUniProtId(row));
            pp.setUniProtCoordsStart(RowUtils.getUniProtStart(row));
            pp.setUniProtCoordsEnd(RowUtils.getUniProtEnd(row));

            // Coordinates in 3D (if any)
            if ( RowUtils.isPDBStructure(row) ) {
                pp.setExperimental(true);
                pp = PeptideRangeTool.setStructureFromRow(pp, row);
            } else {
                pp = PeptideRangeTool.setTemplateFromRow(mapper, pp, row);
            }
            mapping.add(pp);
        }
        return mapping;
    }

    public static void main(String[] args) throws IOException {

        String path = DataLocationProvider.getExonsProjectData() + "/exons_of_interesting_genes.txt";
        List<String> lines = Files.readAllLines(Paths.get(path));
        for (String line : lines) {
            String chr = line.split(",")[2];
            String r1 = line.split(",")[3];
            String r2 = line.split(",")[4];
            NGLScriptsGeneration.createForBoundariesPair(chr, r1, r2);
        }
    }
}