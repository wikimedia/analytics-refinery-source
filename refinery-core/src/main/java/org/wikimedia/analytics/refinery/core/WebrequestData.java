package org.wikimedia.analytics.refinery.core;

/**
 * POJO That encapsulates data from webrequest
 *
 */
public class WebrequestData {
    private final String uriHost;
    private final String uriPath;
    private final String uriQuery;
    private final String httpStatus;
    private final String contentType;
    private final String userAgent;
    private final String rawXAnalyticsHeader;

    public WebrequestData(String uriHost, String uriPath, String uriQuery,
                          String httpStatus, String contentType, String userAgent,
                          String rawXAnalyticsHeader){
        this.uriHost = uriHost.toLowerCase();
        this.uriPath = uriPath;
        this.uriQuery = uriQuery;
        this.httpStatus = httpStatus;
        this.contentType = contentType;
        this.userAgent = userAgent;
        if (rawXAnalyticsHeader == null){
            rawXAnalyticsHeader = "";
        }
        this.rawXAnalyticsHeader = rawXAnalyticsHeader;
    }

    public String getUriHost(){
        return uriHost;
    }

    public String getUriPath(){
        return uriPath;
    }

    public String getUriQuery(){
        return uriQuery;
    }

    public String getHttpStatus(){
        return httpStatus;
    }

    public String getContentType(){
        return contentType;
    }

    public String getUserAgent(){
        return userAgent;
    }

    public String getRawXAnalyticsHeader(){
        return rawXAnalyticsHeader;
    }
}
