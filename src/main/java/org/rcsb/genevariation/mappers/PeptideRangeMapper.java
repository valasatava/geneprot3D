package org.rcsb.genevariation.mappers;

import com.google.common.collect.Range;
import freemarker.template.Template;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.structure.*;
import org.rcsb.exonscoassociation.tools.PeptideRangeTool;
import org.rcsb.exonscoassociation.utils.RowUtils;
import org.rcsb.exonscoassociation.utils.StructureUtils;
import org.rcsb.genevariation.datastructures.PeptideRange;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.tools.TemplatesGenerationTool;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * Created by yana on 5/9/17.
 */
public class PeptideRangeMapper {

    private static final Logger logger = LoggerFactory.getLogger(PeptideRangeMapper.class);

    private static final String DEFAULT_MAPPING_LOCATION = DataLocationProvider.getGencodeStructuralMappingLocation();
    private String MAPPING_LOCATION;

    public PeptideRangeMapper() {
        MAPPING_LOCATION = DEFAULT_MAPPING_LOCATION;
    }

    public PeptideRangeMapper(String LOCATION) {
        MAPPING_LOCATION = LOCATION;
    }

    public List<PeptideRange> getMappingForRange(String chr, int start, int end) throws Exception {
        Dataset<Row> chrom = SaprkUtils.getSparkSession().read().parquet(MAPPING_LOCATION + "/" + chr).cache();
        return getMappingForRange(chrom, start, end);
    }

    public List<PeptideRange> getMappingForRange(Dataset<Row> chrom, int start, int end) {

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

            if ( !RowUtils.getPdbId(row).equals("null") ) {

                List<Group> groups;
                Range<Integer> range;

                if (pp.isExperimental()) {

                    pp.setStructureId(RowUtils.getPdbId(row) + "_" + RowUtils.getChainId(row));
                    range = RowUtils.getPdbRange(row);

                    Structure structure = null;
                    try {structure = StructureUtils.getBioJavaStructure(RowUtils.getPdbId(row)); }
                    catch (IOException ioe) {
                        logger.error("Cannot download structure: " + RowUtils.getPdbId(row));
                        continue; }
                    catch (StructureException stre) {
                        logger.error("StructureException for: " + RowUtils.getPdbId(row));
                        continue; }

                    float resolution = structure.getPDBHeader().getResolution();
                    pp.setResolution(resolution);

                    Chain chain = structure.getPolyChainByPDB(RowUtils.getChainId(row));
                    groups = chain.getAtomGroups();
                    List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups, range.lowerEndpoint(),
                            range.upperEndpoint());

                    pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
                    pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size() - 1).getResidueNumber().getSeqNum());
                    pp.setStructure(groupsInRange);

                } else {

                    pp.setStructureId(RowUtils.getTemplate(row)+".pdb");

                    try { RowUtils.setUTMmapperFromRow(mapper, row); }

                    catch (IOException ioe) {
                        logger.error("Cannot find coordinates: " + mapper.getCoordinates());
                        continue; }

                    catch (CompoundNotFoundException cnfe){
                        logger.error("Is not protein sequence: " + mapper.getAlignment());
                        continue; }

                    range = RowUtils.getModelRange(mapper, row);
                    try { groups = StructureUtils.getGroupsFromModel(mapper.getCoordinates()); }
                    catch (IOException ioe) {
                        logger.error("Cannot find coordinates: " + mapper.getCoordinates());
                        continue; }

                    List<Group> groupsInRange = StructureUtils.getGroupsInRange(groups, range.lowerEndpoint(), range.upperEndpoint());
                    if (groupsInRange.size() != 0) {
                        pp.setStructuralCoordsStart(groupsInRange.get(0).getResidueNumber().getSeqNum());
                        pp.setStructuralCoordsEnd(groupsInRange.get(groupsInRange.size() - 1).getResidueNumber().getSeqNum());
                        pp.setStructure(groupsInRange);
                    }
                }
            }
            mapping.add(pp);
        }
        return mapping;
    }

    public static void main(String[] args) throws Exception {

        PeptideRangeMapper mapper = new PeptideRangeMapper();

        Dataset<Row> chromosome = SaprkUtils.getSparkSession().read()
                .parquet(mapper.MAPPING_LOCATION + "/" + "chr8")
                .distinct().cache();

        List<PeptideRange> mapping1 = null;
        List<PeptideRange> mapping2 = null;
        try {
            String r1 = "27516320_27516398";
            mapping1 = mapper.getMappingForRange(chromosome, Integer.valueOf(r1.split("_")[0]),
                    Integer.valueOf(r1.split("_")[1]));

            String r2 = "27538659_27538692";
            mapping2 = mapper.getMappingForRange(chromosome, Integer.valueOf(r2.split("_")[0]),
                    Integer.valueOf(r2.split("_")[1]));
        } catch (Exception e) {

        }

        Tuple2<PeptideRange, PeptideRange> pair = PeptideRangeTool.getExonsPairWithBestStructuralCoverage(mapping1, mapping2);
        Tuple2<Atom, Atom> atoms = PeptideRangeTool.getClosestBackboneAtoms(pair._1, pair._2);

        /* Create a data-model for exons*/
        Map model = new HashMap();
        if (pair._1.isExperimental()) { model.put("source", "rcsb://"+pair._1.getPdbId()+".mmtf"); }
        else { model.put("source", DataLocationProvider.getHumanHomologyCoordinatesLocation()+"/"+pair._1.getStructureId()); }
        model.put("chain", pair._1.getChainId());
        model.put("start1", pair._1.getStructuralCoordsStart());
        model.put("end1", pair._1.getStructuralCoordsEnd());
        model.put("start2", pair._2.getStructuralCoordsStart());
        model.put("end2", pair._2.getStructuralCoordsEnd());
        model.put("resn1", atoms._1.getGroup().getResidueNumber());
        model.put("resn2", atoms._2.getGroup().getResidueNumber());

        /* Merge data-model with template */
        TemplatesGenerationTool templateTool = new TemplatesGenerationTool();
        String path = DataLocationProvider.getProjectsHome() + "/test.out";
        Template template = templateTool.getNGLtemplate();
        templateTool.writeModelToTemplate(model, template, path);
    }
}