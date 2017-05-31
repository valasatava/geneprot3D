package org.rcsb.geneprot.common.tools;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.rcsb.geneprot.common.io.DataLocationProvider;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/** Creates the templates.
 *
 * Created by Yana Valasatava on 5/24/17.
 */
public class TemplatesGenerationTool {

    private static Configuration cfg;

    public TemplatesGenerationTool() throws IOException {

        cfg = new Configuration();
        cfg.setDefaultEncoding("UTF-8");
        cfg.setDirectoryForTemplateLoading(new File( "." ));
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }

    private static Configuration getConfiguration() {
        return cfg;
    }

    public Template getNGLtemplate() throws IOException {
        return getConfiguration().getTemplate("/templates/ngl_template.js");
    }

    public static void writeModelToTemplate(Map model, Template template, String path) throws IOException, TemplateException {

        OutputStream outputStream = new FileOutputStream(path);
        Writer out = new OutputStreamWriter(outputStream);
        template.process(model, out);
        out.close();
    }

    public static void main(String[] args) throws Exception {

        /* Create a data-model */
        Map model = new HashMap();
        model.put("source", "rcsb://1ZD3.mmtf");
        model.put("chain", "A");
        model.put("start1", 276);
        model.put("end1", 302);
        model.put("start2", 413);
        model.put("end2", 425);
        model.put("resn1", 294);
        model.put("resn2", 418);

        /* Merge data-model with template */
        TemplatesGenerationTool templateTool = new TemplatesGenerationTool();
        String path = DataLocationProvider.getProjectsHome() + "/test.js";
        Template template = templateTool.getNGLtemplate();
        templateTool.writeModelToTemplate(model, template, path);
    }
}