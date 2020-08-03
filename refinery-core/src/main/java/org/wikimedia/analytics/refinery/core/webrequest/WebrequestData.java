package org.wikimedia.analytics.refinery.core.webrequest;

import org.json.simple.JSONObject;
import org.wikimedia.analytics.refinery.core.Utilities;

import java.util.HashMap;
import java.util.Map;

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
    private final boolean isAppUserAgent;

    public WebrequestData(String uriHost, String uriPath, String uriQuery,
                          String httpStatus, String contentType, String userAgent,
                          String rawXAnalyticsHeader){


        if (uriHost != null) {
            this.uriHost = uriHost.toLowerCase().trim();
        } else {
            this.uriHost = uriHost;
        }

        this.uriPath = uriPath;
        this.uriQuery = uriQuery;
        this.httpStatus = httpStatus;
        this.contentType = contentType;
        this.userAgent = userAgent;

        if (rawXAnalyticsHeader == null){
            rawXAnalyticsHeader = "";
        }

        this.rawXAnalyticsHeader = rawXAnalyticsHeader;

        this.isAppUserAgent = Utilities.stringContains(userAgent, "WikipediaApp");
    
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
    
    public boolean isAppUserAgent(){ return isAppUserAgent; }
    
    @Override
    public String toString(){
        Map webrequestMap = new HashMap<String, String>();

        webrequestMap.put("uriHost", this.uriHost);
        webrequestMap.put("uriPath", this.uriPath);
        webrequestMap.put("uriQuery", this.uriQuery);
        webrequestMap.put("httpStatus", this.httpStatus);
        webrequestMap.put("contentType", this.contentType);
        webrequestMap.put("userAgent", this.userAgent);
        webrequestMap.put("X-Analytics", this.rawXAnalyticsHeader);

        return JSONObject.toJSONString(webrequestMap);
    }

}
