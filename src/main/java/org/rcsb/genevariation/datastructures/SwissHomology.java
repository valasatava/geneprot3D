package org.rcsb.genevariation.datastructures;

import org.json.JSONObject;

import java.io.Serializable;

/**
 * Created by Ali on 11/28/16.
 * This class is used to create Parquet files using Spark dataframe.
 */
public class SwissHomology implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -5809837612200436472L;

	private String uniProtId;

    private Integer fromPos;
    private Integer toPos;

    private String alignment;
    private String coordinates;
    private String crc64;
    private Double gmqe;
    private Double identity;
    private String md5;
    private String method;
    private String oligo_state;
    private String provider;
    private Double qmean;
    private Double similarity;
    private String template;

    public String getUniProtId() {
        return uniProtId;
    }

    public void setUniProtId(String uniProtId) {
        this.uniProtId = uniProtId;
    }

    public Integer getFromPos() {
        return fromPos;
    }

    public void setFromPos(Integer fromPos) {
        this.fromPos = fromPos;
    }

    public Integer getToPos() {
        return toPos;
    }

    public void setToPos(Integer toPos) {
        this.toPos = toPos;
    }

    public String getAlignment() {
        return alignment;
    }

    public void setAlignment(String alignment) {
        this.alignment = alignment;
    }

    public String getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(String coordinates) {
        this.coordinates = coordinates;
    }

    public String getCrc64() {
        return crc64;
    }

    public void setCrc64(String crc64) {
        this.crc64 = crc64;
    }

    public Double getGmqe() {
        return gmqe;
    }

    public void setGmqe(Double gmqe) {
        this.gmqe = gmqe;
    }

    public Double getIdentity() {
        return identity;
    }

    public void setIdentity(Double identity) {
        this.identity = identity;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getOligo_state() {
        return oligo_state;
    }

    public void setOligo_state(String oligo_state) {
        this.oligo_state = oligo_state;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public Double getQmean() {
        return qmean;
    }

    public void setQmean(Double qmean) {
        this.qmean = qmean;
    }

    public Double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(Double similarity) {
        this.similarity = similarity;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public void setModelFromJSONObject(JSONObject homologyObject) {

        this.setFromPos(homologyObject.getInt("from"));
        this.setToPos(homologyObject.getInt("to"));
        this.setAlignment(homologyObject.getString("alignment"));
        this.setCoordinates(homologyObject.getString("coordinates"));

        this.setCrc64(homologyObject.has("crc64") ? homologyObject.getString("crc64") : null);
        this.setGmqe(homologyObject.has("gmqe") ? homologyObject.getDouble("gmqe") : null);
        this.setIdentity(homologyObject.has("identity") ? homologyObject.getDouble("identity") : null);
        this.setMd5(homologyObject.has("md5") ? homologyObject.getString("md5") : null);
        this.setMethod(homologyObject.has("method") ? homologyObject.getString("method") : null);
        this.setOligo_state(homologyObject.has("oligo-state") ? homologyObject.getString("oligo-state") : null);
        this.setProvider(homologyObject.has("provider") ? homologyObject.getString("provider") : null);
        this.setQmean(homologyObject.has("qmean") ? homologyObject.getDouble("qmean") : null);
        this.setSimilarity(homologyObject.has("similarity") ? homologyObject.getDouble("similarity") : null);
        this.setTemplate(homologyObject.has("template") ? homologyObject.getString("template") : null);

    }
}
