package org.wikimedia.analytics.refinery.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jo on 6/3/15.
 */
public class NormalizedHostInfo {

    /*
     * Constant string for empty values of normalized host info
     */
    public static final String EMPTY_NORM_HOST_VALUE  = "-";

    private String projectFamily;
    private String project;
    private List<String> qualifiers;
    private String tld;

    public NormalizedHostInfo() {
        projectFamily = EMPTY_NORM_HOST_VALUE;
        project = EMPTY_NORM_HOST_VALUE;
        qualifiers = new ArrayList<>();
        tld = EMPTY_NORM_HOST_VALUE;
    }

    public String getProjectFamily() {
        return projectFamily;
    }

    public void setProjectFamily(String projectFamily) {
        this.projectFamily = projectFamily;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public List<String> getQualifiers() {
        return qualifiers;
    }

    public void addQualifier(String qualifier) {
        this.qualifiers.add(qualifier);
    }

    public String getTld() {
        return tld;
    }

    public void setTld(String tld) {
        this.tld = tld;
    }
}
