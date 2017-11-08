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

import java.util.*;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.springframework.security.web.util.matcher.IpAddressMatcher;

public class IpUtil {

    /**
     * List of trusted proxies
     * <p>
     * The following trusted proxies list is sourced from
     * https://phabricator.wikimedia.org/source/operations-puppet/browse/master/manifests/network.pp;9f97e3c2c5bc012ba5c3751f13fd838a06d6528d$14
     * For now, any updates to this source must be manually brought over here.
     */
    final String[] trustedProxies = new String[] {
            "91.198.174.0/24",
            "208.80.152.0/22",
            "2620:0:860::/46",
            "198.35.26.0/23",
            "185.15.56.0/22",
            "2a02:ec80::/32",
            "10.0.0.0/8"
    };

    Set<IpAddressMatcher> trustedProxiesCache;

    /**
     * List of Wikimedia Labs subnets
     * <p>
     * The following list is sourced from ops/puppet.git's
     * $all_network_subnets global variable. Specifically these were taken
     * from manifests/network.pp at git hash bc1d7ef.
     * @see https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/manifests/network.pp
     */
    final String[] labsSubnets = new String[] {
            // labs-instances1-a-eqiad
            "10.68.0.0/24",
            "2620:0:861:201::/64",
            // labs-instances1-b-eqiad
            "10.68.16.0/21",
            "2620:0:861:202::/64",
            // labs-instances1-c-eqiad
            "10.68.32.0/24",
            "2620:0:861:203::/64",
            // labs-instances1-d-eqiad
            "10.68.48.0/24",
            "2620:0:861:204::/64",
            // labs-hosts1-a-eqiad
            "10.64.4.0/24",
            "2620:0:861:117::/64",
            // labs-hosts1-b-eqiad
            "10.64.20.0/24",
            "2620:0:861:118::/64",
            // labs-hosts1-d-eqiad
            "10.64.52.0/24",
            // labs-support1-c-eqiad
            "10.64.37.0/24",
            "2620:0:861:119::/64"
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
     * https://git.wikimedia.org/blob/operations%2Fpuppet.git/9f97e3c2c5bc012ba5c3751f13fd838a06d6528d/manifests%2Fnetwork.pp#L14
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
    public NetworkOrigin getNeworkOrigin(final String ip) {
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
}
