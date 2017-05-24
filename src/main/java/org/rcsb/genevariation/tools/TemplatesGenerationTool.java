package org.rcsb.genevariation.tools;

import freemarker.template.Configuration;

/**
 * Created by yana on 5/24/17.
 */
public class TemplatesGenerationTool {

    public static void main(String[] args) throws Exception {

        Configuration cfg = new Configuration();

        // Where do we load the templates from:
        cfg.setClassForTemplateLoading(TemplatesGenerationTool.class, "/");

    }

}
