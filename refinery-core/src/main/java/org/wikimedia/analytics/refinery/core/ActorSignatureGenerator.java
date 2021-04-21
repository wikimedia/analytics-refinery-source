/*
 * Copyright (C) 2020  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.analytics.refinery.core;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;

/**
 * Computes a traffic actor signature.
 *
 * The signature is computed as:
 *
 * md5(
 *     concat(
 *       ip,
 *       substr(user_agent,0,200),
 *       accept_language,
 *       uri_host,
 *       COALESCE(x_analytics_map['wmfuuid'], parse_url(concat('', uri_query), 'QUERY', 'appInstallID'),'')
 *     )
 *   )
 */
public class ActorSignatureGenerator implements Serializable {

    // Serializable objects
    private final String WMFUUID = "wmfuuid";
    private final String APP_ID = "appInstallID";
    private final StringBuilder message;

    // Non serializable object - Transient
    private transient MessageDigest digest;

    private void initializeDigestIfNeeded() {
        if (digest == null) {
            try {
                digest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public ActorSignatureGenerator() {
        message = new StringBuilder();
        initializeDigestIfNeeded();
    }

    private String getMessage(String ip, String userAgent, String acceptLanguage, String uriHost,
                              String uriQuery, Map<String, String> xAnalyticsMap) {

        message.setLength(0);
        message.append(ip);
        message.append(userAgent.substring(0, Math.min(200, userAgent.length())));
        message.append(acceptLanguage);
        message.append(uriHost);
        // If wmfuuid is defined in x_analytics header use it
        if (xAnalyticsMap != null && xAnalyticsMap.containsKey(WMFUUID)) {
            message.append(xAnalyticsMap.get(WMFUUID));
        } else {
            // Otherwise try to extract app-install-id from uriQuery
            int idxStartPattern = (uriQuery == null) ? -1 : uriQuery.indexOf(APP_ID);
            if (idxStartPattern > 0) {
                int idxEndPattern = uriQuery.indexOf('&', idxStartPattern);
                idxEndPattern = (idxEndPattern == -1) ? uriQuery.length() : idxEndPattern;
                message.append(uriQuery.substring(idxStartPattern + APP_ID.length() + 1, idxEndPattern));
            }
        }

        return message.toString();
    }

    public String execute(String ip, String userAgent, String acceptLanguage, String uriHost,
                          String uriQuery, Map<String, String> xAnalyticsMap) {

        // Mimic HIVE function returning null if any non-coalesced string value is null
        if (ip == null || userAgent == null || acceptLanguage == null || uriHost == null) {
            return null;
        }

        String message = getMessage(ip, userAgent, acceptLanguage, uriHost, uriQuery, xAnalyticsMap);

        // Force initialization in case serialization has happened, making message and digest null
        initializeDigestIfNeeded();

        // Copied from https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/UDFMd5.java
        digest.reset();
        digest.update(message.getBytes(), 0, message.length());
        byte[] md5Bytes = digest.digest();
        return Hex.encodeHexString(md5Bytes);

    }

}
