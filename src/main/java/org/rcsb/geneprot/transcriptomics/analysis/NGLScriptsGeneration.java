package org.rcsb.geneprot.transcriptomics.analysis;

import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Calc;
import org.rcsb.geneprot.common.datastructures.PeptideRange;
import org.rcsb.geneprot.common.io.DataLocationProvider;
import org.rcsb.geneprot.common.mappers.PeptideRangeMapper;
import org.rcsb.geneprot.common.tools.PeptideRangeTool;
import org.rcsb.geneprot.common.tools.TemplatesGenerationTool;
import org.rcsb.geneprot.common.utils.SaprkUtils;
import org.rcsb.geneprot.transcriptomics.datastructures.ExonBoundariesPair;
import org.rcsb.geneprot.transcriptomics.io.CustomDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Created by yana on 5/25/17.
 */
public class NGLScriptsGeneration {

    private static final Logger logger = LoggerFactory.getLogger(NGLScriptsGeneration.class);

    public static void createForBoundariesPair(String chr, String exon1, String exon2) {

         /* Get the mapping */
        Dataset<Row> chromosome = SaprkUtils.getSparkSession().read()
                .parquet(DataLocationProvider.getGencodeStructuralMappingLocation() + "/" + chr)
                .distinct().cache();

        /* Get structural mapping for exons */

        PeptideRangeMapper mapper = new PeptideRangeMapper();

        List<PeptideRange> mapping1 = mapper.mapGeneticCoordinatesToPeptideRange(chromosome, Integer.valueOf(exon1.split("_")[0]),
                Integer.valueOf(exon1.split("_")[1]));
        List<PeptideRange> mapping2 = mapper.mapGeneticCoordinatesToPeptideRange(chromosome, Integer.valueOf(exon2.split("_")[0]),
                Integer.valueOf(exon2.split("_")[1]));

        Tuple2<PeptideRange, PeptideRange> pair = PeptideRangeTool.getBestPair(mapping1, mapping2);

        if ( pair == null )
            return;

        /* Get closest atoms for two exons */
        Tuple2<Atom, Atom> atoms = PeptideRangeTool.getClosestBackboneAtoms(pair._1, pair._2);

        /* Create a data-model for exons */
        Map model = PeptideRangeTool.getModelForPeptidePair(pair);
        model.put("resn1", atoms._1.getGroup().getResidueNumber());
        model.put("resn2", atoms._2.getGroup().getResidueNumber());

        /* Merge data-model with template */
        TemplatesGenerationTool templateTool = null;
        try {
            templateTool = new TemplatesGenerationTool();
            double distance = Calc.getDistance(atoms._1, atoms._2);
            String path = DataLocationProvider.getExonsProjectResults() + "/ngl_scripts/"+pair._1.getGeneName()+"_"+chr+"_"+exon1+"-"+exon2+"_"+String.valueOf(Math.round(distance))+".js";
            Template template = templateTool.getNGLtemplate();
            templateTool.writeModelToTemplate(model, template, path);
            logger.info("The template is created for: "+pair._1.getGeneName()+"_"+chr+"_"+exon1+"-"+exon2); }

        catch (IOException e) {
            e.printStackTrace(); }

        catch (TemplateException e) {
            e.printStackTrace(); }
    }

    public static void run() {

        List<ExonBoundariesPair> pairs = null;
        try { pairs = CustomDataProvider.getCoordinatedPairs(
                Paths.get(DataLocationProvider.getExonsProjectData()
                            + "/coordinated_pairs.csv")); }
        catch (IOException e) {
            logger.error("Cannot get coordinated pairs: " + e.getMessage()); }

        for (ExonBoundariesPair pair : pairs) {
            createForBoundariesPair(pair.getChromosome(),
                    pair.getExon1(), pair.getExon2()); }
    }

    public static void main(String[] args) {
         run();
    }
}