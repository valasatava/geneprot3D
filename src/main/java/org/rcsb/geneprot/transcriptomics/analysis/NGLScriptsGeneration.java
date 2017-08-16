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
import org.rcsb.geneprot.common.utils.SparkUtils;
import org.rcsb.geneprot.transcriptomics.datastructures.ExonBoundariesPair;
import org.rcsb.geneprot.transcriptomics.io.CustomDataProvider;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.config.RCSBUniProtMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Created by Yana Valasatava on 5/25/17.
 */
public class NGLScriptsGeneration {

    private static final Logger logger = LoggerFactory.getLogger(NGLScriptsGeneration.class);

    public static void createForBoundariesPair(String chr , String exon1, String exon2) {

        /* Get the mapping */
        Dataset<Row> chromosome = SparkUtils.getSparkSession().read()
                .parquet(DataLocationProvider.getGencodeStructuralMappingLocation() + "/" + chr)
                .distinct().cache();

        PeptideRangeMapper mapper = new PeptideRangeMapper();

        List<PeptideRange> mapping1 = mapper.mapGeneticCoordinatesToPeptideRange(chromosome,
                                                Integer.valueOf(exon1.split("_")[0]),
                                                Integer.valueOf(exon1.split("_")[1]));
        List<PeptideRange> mapping2 = mapper.mapGeneticCoordinatesToPeptideRange(chromosome,
                                                Integer.valueOf(exon2.split("_")[0]),
                                                Integer.valueOf(exon2.split("_")[1]));
        if (mapping1.size()==0 || mapping2.size()==0) {
            logger.info( mapping1.size()==0 ?
                    chr + " " + exon1 + " cannot be mapped to 3D coordinates" :
                    chr + " " + exon2 + " cannot be mapped to 3D coordinates");
            return;
        }

        Tuple2<PeptideRange, PeptideRange> pair = PeptideRangeTool.getBestPair(mapping1, mapping2);
        if (pair == null ) {
            logger.info(chr + " " + exon1 + " " + exon2 + " do not have a structure in common");
            return;
        }

        /* Get closest atoms for two exons */
        Tuple2<Atom, Atom> atoms = PeptideRangeTool.getClosestBackboneAtoms(pair._1, pair._2);
        pair._1.addOtherResidue(atoms._1.getGroup().getResidueNumber().getSeqNum());
        pair._2.addOtherResidue(atoms._2.getGroup().getResidueNumber().getSeqNum());

        /* Get UniProt annotation */
        try { Uniprot up = RCSBUniProtMirror.getUniProtFromFile(pair._1.getUniProtId());

            PeptideRangeTool.annotateResidues(pair._1, up, "active site");
            PeptideRangeTool.annotateResidues(pair._2, up, "active site"); }

        catch (FileNotFoundException e) {
            e.printStackTrace(); }

        catch (JAXBException e) {
            e.printStackTrace(); }

        /* Create a data-model for exons */
        Map model = PeptideRangeTool.getModelForPeptidePair(pair);

        /* Merge data-model with template */
        TemplatesGenerationTool templateTool = null;
        try { templateTool = new TemplatesGenerationTool();

            int distance1D = (pair._2.getUniProtCoordsStart() > pair._1.getUniProtCoordsEnd()) ?
                    pair._2.getUniProtCoordsStart() - pair._1.getUniProtCoordsEnd() :
                    pair._1.getUniProtCoordsStart() - pair._2.getUniProtCoordsEnd();
            double distance3D = Calc.getDistance(atoms._1, atoms._2);

            String pathToTemplate = DataLocationProvider.getExonsProjectResults() + "/ngl_scripts/"+
                    pair._1.getGeneName()+"_"+pair._1.getChromosome()+"_"+exon1+"-"+exon2+"_"+
                    String.valueOf(distance1D)+"_"+
                    String.valueOf(Math.round(distance3D))+".js";

            Template template = templateTool.getNGLtemplate();
            templateTool.writeModelToTemplate(model, template, pathToTemplate);
            logger.info("The template is created for: "+
                    pair._1.getGeneName()+"_"+pair._1.getChromosome()+"_"+exon1+"-"+exon2); }

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
            if ( ! ( pair.getChromosome().equals("chr2") &&
                     pair.getExon1().equals("121409024_121409047") &&
                     pair.getExon2().equals("121444939_121444962")) )
                continue;
            createForBoundariesPair(pair.getChromosome(),
                    pair.getExon1(), pair.getExon2()); }
    }

    public static void main(String[] args) {
        run();
        SparkUtils.getSparkSession().stop();
    }
}