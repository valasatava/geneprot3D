package exonscorrelation;

import org.biojava.nbio.core.alignment.template.SequencePair;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.ProteinSequence;
import org.biojava.nbio.core.sequence.compound.AminoAcidCompound;
import org.rcsb.uniprot.auto.Uniprot;
import org.rcsb.uniprot.auto.tools.UniProtTools;
import org.rcsb.uniprot.isoform.IsoformMapper;
import org.rcsb.uniprot.isoform.IsoformTools;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class UniprotMapping {

	public static String getIsoform(String up, int ind) throws IOException, JAXBException, CompoundNotFoundException {

		URL u = UniProtTools.getURLforXML(up);
		InputStream inStream = u.openStream();
		Uniprot uniprot = UniProtTools.readUniProtFromInputStream(inStream);

		IsoformTools tools = new IsoformTools();

		ProteinSequence[] allIsoforms = tools.getIsoforms(uniprot);
		ProteinSequence isoform = allIsoforms[ind];

		return isoform.getSequenceAsString();
	}

	public static void main(String[] args) throws IOException, JAXBException, CompoundNotFoundException {

		String up = "Q7Z6I6";

		URL u = UniProtTools.getURLforXML(up);
		InputStream inStream = u.openStream();
		Uniprot uniprot = UniProtTools.readUniProtFromInputStream(inStream);

		IsoformTools tools = new IsoformTools();

		ProteinSequence[] allIsoforms = tools.getIsoforms(uniprot);

		ProteinSequence canonical = allIsoforms[0];
		System.out.println(canonical.getSequenceAsString());

		ProteinSequence isoform = allIsoforms[2];
		System.out.println(isoform.getSequenceAsString());

		System.out.println();

		IsoformMapper mapper = new IsoformMapper(uniprot, canonical, isoform);
		SequencePair<ProteinSequence, AminoAcidCompound> pair = mapper.getPair();

		System.out.println(pair.getQuery());
		System.out.println(pair.getTarget());

//		System.out.println(mapper.convertPos2toPos1(229));
//		System.out.println(mapper.convertPos2toPos1(230));
//		System.out.println(mapper.convertPos2toPos1(231));

	}
}
