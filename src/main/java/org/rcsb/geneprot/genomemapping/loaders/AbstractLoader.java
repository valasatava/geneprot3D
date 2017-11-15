package org.rcsb.geneprot.genomemapping.loaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Yana Valasatava on 11/7/17.
 */
public abstract class AbstractLoader {

    private static final Logger logger = LoggerFactory.getLogger(AbstractLoader.class);

    public static String format;
    public static int taxonomyId;

    public static void setArguments(String[] args) {

        if (args.length > 0) {

            try {
                String taxonomy = args[0];
                taxonomyId = Integer.valueOf(taxonomy);
                logger.info("Taxonomy ID of the specie: {}", taxonomyId);
            } catch (Exception e) {
                logger.error("Taxonomy ID is incorrect");
            }

            if (args.length > 1) {
                try {
                    format = args[1];
                } catch (Exception e) {
                    logger.error("Format of annotation file is invalid");
                }
            }

        } else {
            logger.error("Arguments are not set");
        }
    }

    public static String getFormat() {
        return format;
    }

    public static int getTaxonomyId() {
        return taxonomyId;
    }

    public static String getOrganism() {
        if (taxonomyId==9606)
            return "human";
        return "";
    }

}
