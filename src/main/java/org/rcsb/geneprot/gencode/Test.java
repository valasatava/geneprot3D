package org.rcsb.geneprot.gencode;

import org.rcsb.geneprot.common.utils.CommonUtils;
import org.rcsb.geneprot.gencode.dao.MetadataDAO;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by Yana Valasatava on 8/7/17.
 */
public class Test {



    public static void main(String[] args) throws IOException {

        Map<String, String> genes = MetadataDAO.getGeneSymbols();
        Map<String, String> uniprotIds = MetadataDAO.getGeneSwissProt();

        Set<String> keys = CommonUtils.getKeysByValue(genes, "Mrpl15");
        for (String transcriptId : keys) {
            System.out.println(transcriptId);
            System.out.println(uniprotIds.get(transcriptId));
        }
    }
}
