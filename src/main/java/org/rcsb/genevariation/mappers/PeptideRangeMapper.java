package org.rcsb.genevariation.mappers;

import com.google.common.collect.Range;
import freemarker.template.Template;
import freemarker.template.TemplateException;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public List<PeptideRange> getMappingForRange(String chr, int start, int end) throws Exception {
        Dataset<Row> chrom = SaprkUtils.getSparkSession().read().parquet(MAPPING_LOCATION + "/" + chr).cache();
        return getMappingForRange(chrom, start, end);
    }

    public List<PeptideRange> getMappingForRange(Dataset<Row> chrom, int start, int end) {

        Dataset<Row> data = chrom.filter(col("start").equalTo(start).and(col("end").equalTo(end)));
        List<Row> dataList = data.collectAsList();

        List<PeptideRange> mapping = new ArrayList<>();

        for (Row row : dataList ) {

            PeptideRange pp = new PeptideRange();

            pp.setChromosome(RowUtils.getChromosome(row));
            pp.setEnsemblId(RowUtils.getEnsemblId(row));
            pp.setGeneBankId(RowUtils.getGeneBankId(row));
            pp.setGeneName(RowUtils.getGeneName(row));
            pp.setGenomicCoordsStart(start);
            pp.setGenomicCoordsEnd(end);

            pp.setUniProtId(RowUtils.getUniProtId(row));
            pp.setUniProtCoordsStart(RowUtils.getUniProtStart(row));
            pp.setUniProtCoordsEnd(RowUtils.getUniProtEnd(row));

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

    public static void main(String[] args) {

        PeptideRangeMapper mapper = new PeptideRangeMapper();

        String chr = "chr8";
        String r1 = "27516320_27516398";
        String r2 = "27538659_27538692";

        Dataset<Row> chromosome = SaprkUtils.getSparkSession().read()
                .parquet(mapper.MAPPING_LOCATION + "/" + chr)
                .distinct().cache();

        List<PeptideRange> mapping1 = mapper.getMappingForRange(chromosome, Integer.valueOf(r1.split("_")[0]),
                    Integer.valueOf(r1.split("_")[1]));

        List<PeptideRange> mapping2 = mapper.getMappingForRange(chromosome, Integer.valueOf(r2.split("_")[0]),
                    Integer.valueOf(r2.split("_")[1]));

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
        TemplatesGenerationTool templateTool = null;
        try {
            templateTool = new TemplatesGenerationTool();
            String path = DataLocationProvider.getExonsProjectResults() + "/ngl_scripts/"+chr+"_"+r1+"_"+r2+".js";
            Template template = templateTool.getNGLtemplate();
            templateTool.writeModelToTemplate(model, template, path);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TemplateException e) {
            e.printStackTrace();
        }
    }
}