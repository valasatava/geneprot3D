package org.rcsb.genevariation.sandbox;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.util.InputStreamProvider;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePositionParser;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.biojava.nbio.genome.util.ProteinMappingTools;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.auto.tools.UniProtTools;
import org.rcsb.uniprot.isoform.IsoformTools;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

public class Test {

	private static final String DEFAULT_MAPPING_URL="http://hgdownload.cse.ucsc.edu/goldenPath/hg38/database/refFlat.txt.gz";

	public static void main(String[] args) throws Exception {
		testIsoform();
	}

	public static void test() throws Exception {
		List<GeneChromosomePosition> gcps = GeneChromosomePositionParser.getChromosomeMappings();
		for (GeneChromosomePosition gcp : gcps) {
			if (gcp.getGeneName().equals("YBX2") && gcp.getGenebankId().equals("NM_015982")) {
				int pos = ChromosomeMappingTools.getCDSPosForChromosomeCoordinate(7192850, gcp);
				System.out.println(pos);
			}
		}
	}

	public static void testIsoform() throws Exception {

		File f = new File(System.getProperty("user.home")+"/data/genevariation/hg38.2bit");
		TwoBitFacade twoBitFacade = new TwoBitFacade(f);

		URL url = new URL(DEFAULT_MAPPING_URL);
		InputStreamProvider prov = new InputStreamProvider();
		InputStream inStreamGenes = prov.getInputStream(url);
		List<GeneChromosomePosition> gcps = GeneChromosomePositionParser.getChromosomeMappings(inStreamGenes);

		String uniProtId = "Q02161";

		URL u = UniProtTools.getURLforXML(uniProtId);
		InputStream inStream = u.openStream();
		Uniprot uniprot = UniProtTools.readUniProtFromInputStream(inStream);

		IsoformTools tools = new IsoformTools();

		ProteinSequence[] isoforms = tools.getIsoforms(uniprot);

		for (GeneChromosomePosition gcp : gcps) {

			if (!(gcp.getChromosome().equals("chr1") && gcp.getGeneName().equals("RHD")))
				continue;

			DNASequence transcriptDNASequence = ChromosomeMappingTools.getTranscriptDNASequence(twoBitFacade, gcp);
			ProteinSequence sequence = ProteinMappingTools.convertDNAtoProteinSequence(transcriptDNASequence);

			int index = tools.getBestMatchingIsoform(isoforms, sequence);

			System.out.println(index);
		}
	}
}
