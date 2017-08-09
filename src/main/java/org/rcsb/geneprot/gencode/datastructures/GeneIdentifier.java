package org.rcsb.geneprot.gencode.datastructures;

/**
 * Created by Yana Valasatava on 8/7/17.
 */
public interface GeneIdentifier {

    public String getId();
    public String getTranscriptId();

    public String getById(String id);
    public String getByTranscriptId(String id);

}
