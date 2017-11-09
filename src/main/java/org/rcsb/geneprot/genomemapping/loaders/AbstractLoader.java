package org.rcsb.geneprot.genomemapping.loaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Yana Valasatava on 11/7/17.
 */
public abstract class AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(AbstractLoader.class);

    public static int taxonomyId;

    public static void setArguments(String[] args) {
        if (args.length > 0) {
            String taxonomy = args[0];
            try {
                taxonomyId = Integer.valueOf(taxonomy);
                logger.info("Taxonomy ID of the specie: {}", taxonomyId);
            } catch (Exception e) {
                logger.error("Taxonomy ID is incorrect: {}", taxonomy);
            }

        } else {
            logger.error("Arguments are not set. Path to annotation file and taxonomy ID of the organism are required.");
        }
    }

    public static int getTaxonomyId() {
        return taxonomyId;
    }

    public static String getOrganism() {
        if (taxonomyId==9606)
            return "HomoSapience";
        return "";
    }

}
