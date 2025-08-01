/**
 * Copyright (C) 2015 Wikimedia Foundation
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

import java.math.BigInteger;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.springframework.security.web.util.matcher.IpAddressMatcher;

public class IpUtil {

    /**
     * List of trusted proxies.
     * <p>
     * The following trusted proxies list is sourced from
     * https://github.com/wikimedia/puppet/blob/production/modules/network/data/data.yaml
     * For now, any updates to this source must be manually brought over here.
     *
     * Last update: 2019-09-23
     */
    final String[] trustedProxies = new String[] {
            "91.198.174.0/24",
            "208.80.152.0/22",
            "2620:0:860::/46",
            "198.35.26.0/23",
            "185.15.56.0/24",
            "2a02:ec80::/32",
            "2001:df2:e500::/48",
            "103.102.166.0/24",

            "10.0.0.0/8"       // Internal subnet
    };




    Set<IpAddressMatcher> trustedProxiesCache;

    /**
     * List of Wikimedia Cloud Services subnets
     * <p>
     * This list is sourced from the Wikitech [[Help:Cloud VPS IP space]] as well as from Netbox.
     * https://wikitech.wikimedia.org/wiki/Help:Cloud_VPS_IP_space
     *
     * Last updated: 2025-04-24
     */
    final String[] labsSubnets = new String[] {
            // cloud-eqiad1
            "172.16.0.0/17",
            "185.15.56.0/25",
            "2a02:ec80:a000::/56",

            // cloud-codfw1dev
            "172.16.128.0/17",
            "185.15.57.0/29",
            "185.15.57.16/29",
            "2a02:ec80:a100::/56",
    };

    Set<IpAddressMatcher> labsSubnetsCache;

    public enum NetworkOrigin {
        WIKIMEDIA_LABS,
        WIKIMEDIA,
        INTERNET;

        public String toString() {
            return name().toLowerCase();
        }
    }

    /**
     * Constructs a IpUtil object with the default list of trusted proxies
     * <p>
     * The default list of trusted proxies is sourced from:
     * https://phabricator.wikimedia.org/source/operations-puppet/browse/production/manifests/network.pp;9f97e3c2c5bc012ba5c3751f13fd838a06d6528d$14
     */
    public IpUtil() {
        trustedProxiesCache = new HashSet<IpAddressMatcher>();
        labsSubnetsCache = new HashSet<IpAddressMatcher>();

        for (String trustedProxy: trustedProxies) {
            IpAddressMatcher matcher = new IpAddressMatcher(trustedProxy);
            trustedProxiesCache.add(matcher);
        }

        for (String labsSubnet: labsSubnets) {
            IpAddressMatcher matcher = new IpAddressMatcher(labsSubnet);
            labsSubnetsCache.add(matcher);
        }
    }

    /**
     * Gets the client IP given the source IP and X-Forwarded-For header value
     * @param ip   the remote IP address of the requests
     * @param xff  Comma-separated list of ip addresses in X-Forwarded-For header
     * @return String Client IP address (trimmed, if required, but no
     * canonicalization) and {@code null} otherwise.
     */
    public String getClientIp(final String ip, final String xff) {
        String ret = null;

        String sanitizedIp = sanitizeIp(ip);

        if (sanitizedIp != null) {
            // The original ip is good
            ret = sanitizedIp;

            if (isTrustedProxy(ret) && xff != null) {
                // ip was a proxy, and xff is not null.
                // Trying to detect user's ip by backtracking on xff.
                String[] xffIps = xff.split(",");

                // As proxies append (not prepend) ips to xff, we need to
                // traverse xff from right to left. We do so by reversing
                // xffIps and then iterating normally over it.
                Collections.reverse(Arrays.asList(xffIps));
                for (String xffIp : xffIps) {
                    sanitizedIp = sanitizeIp(xffIp);
                    if (sanitizedIp == null) {
                        // The current xffIp is not a proper IP. Hence bailing
                        // out and moving forward with last known good IP
                        // in ret.
                        break;
                    }
                    // The ip got sanitized, so we mark it as best possible
                    // return value for now.
                    ret = sanitizedIp;
                    if (!isTrustedProxy(ret)) {
                        // ret is not a trusted proxy, so we have to stop
                        // iterating further to avoid using spoofed entries.
                        break;
                    }
                }
            }
        }
        return ret;
    }

    /**
     * Gets the network origin for a given IP address.
     * @param ip IP address
     * @return NetworkOrigin Network that the IP belongs to (wikimedia,
     * wikimedia labs, or internet)
     */
    public NetworkOrigin getNetworkOrigin(final String ip) {
        final String sanitizedIp = sanitizeIp(ip);

        if (sanitizedIp != null) {
            if (isLabsHost(sanitizedIp)) {
                return NetworkOrigin.WIKIMEDIA_LABS;

            } else if (isTrustedProxy(sanitizedIp)) {
                return NetworkOrigin.WIKIMEDIA;
            }
        }

        return NetworkOrigin.INTERNET;
    }

    /**
     * Trims and validates the given IP address string
     * <p>
     * Trims the input string and validates whether the resulting string is a
     * valid IPv4 or IPv6 address. However, no canonicalization is done
     * @param ip IP address
     * @return String  sanitized IP address, if the given string is a valid
     * IPv4 or IPv6 address. {@code null} otherwise.
     */
    private static String sanitizeIp(String ip) {
        String sanitizedIp = null;

        if (ip != null) {
            ip = ip.trim();
            if (InetAddressValidator.getInstance().isValid(ip)) {
                // We have a valid non-empty ip address
                sanitizedIp = ip;
            }
        }

        return sanitizedIp;
    }

    /**
     * Checks whether the given IP address matches any of the trusted proxies
     * <p>
     * Assumes that the input IP address is already trimmed and validated
     * @param ip IP address
     * @return Boolean {@code true}, if the given ip address matches any of the
     * trustedProxies. {@code false} otherwise.
     */
    private boolean isTrustedProxy(String ip) {
        boolean isTrusted = false;

        for (IpAddressMatcher ipAddressMatcher : trustedProxiesCache) {
            if (ipAddressMatcher.matches(ip)) {
                // The given ip matches one of the proxies in our list
                isTrusted = true;
                break;
            }
        }
        return isTrusted;
    }

    /**
     * Does the given IP address belong to a Wikimedia Labs hosted instance?
     * @param ip IP address
     * @return Boolean {@code true} when ip matches Labs subnet, {@code false}
     * otherwise.
     */
    private boolean isLabsHost(String ip) {
        boolean isLabs = false;

        for (IpAddressMatcher subnet: labsSubnetsCache) {
            if (subnet.matches(ip)) {
                isLabs = true;
                break;
            }
        }
        return isLabs;
    }

    /**
     * An IPv6 address is made up of 8 words (each x0000 to xFFFF).
     * However, the "::" abbreviation can be used on consecutive x0000 words.
     */
    private static final String RE_IPV6_WORD = "([0-9A-Fa-f]{1,4})";

    /**
     * Converts an IPv4 or IPv6 hexadecimal representation back to readable format.
     *
     * @param ipHex Number, with "v6-" prefix if it is IPv6
     * @return Quad-dotted (IPv4) or octet notation (IPv6)
     */
    public String formatHex(String ipHex) {
        if (ipHex.startsWith("v6-")) {
            // IPv6
            return hexToOctet(ipHex.substring(3));
        }
        // IPv4
        return hexToQuad(ipHex);
    }

    /**
     * Converts a hexadecimal number to an IPv6 address in octet notation.
     *
     * @param ipHex Pure hex (no v6- prefix)
     * @return  (of format a:b:c:d:e:f:g:h)
     */
    public String hexToOctet(String ipHex) {
        // Pad hex to 32 chars (128 bits)
        ipHex = StringUtils.leftPad(ipHex.toUpperCase(), 32, "0");
        // Separate into 8 words
        StringBuilder ipOct = new StringBuilder(ipHex.substring(0, 4));
        for (int n = 1; n < 8; n++) {
            ipOct.append(':').append(ipHex, 4 * n, 4 * n + 4);
        }
        // NO leading zeroes
        return ipOct.toString().replaceAll("(^|:)0+" + RE_IPV6_WORD, "$1$2");
    }
    /**
     * Converts a hexadecimal number to an IPv4 address in quad-dotted notation.
     *
     * @param ipHex Pure hex
     * @return string (of format a.b.c.d)
     */
    public String hexToQuad(String ipHex) {
        // Pad hex to 8 chars (32 bits)
        ipHex = StringUtils.leftPad(ipHex.toUpperCase(), 8, "0");
        // Separate into four quads
        StringBuilder ipOct = new StringBuilder();

        for (int i = 0; i < 4; i++) {
            if (ipOct.length() > 0) {
                ipOct.append('.');
            }
            ipOct.append(new BigInteger(ipHex.substring(i*2, i*2+2), 16).toString(10));
        }
        return ipOct.toString();
    }
}
