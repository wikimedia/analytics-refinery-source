// Copyright 2019 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.wikimedia.analytics.refinery.core.Webrequest;

@Description(name = "get_referer_wiki",
        value = "_FUNC_(url) - Returns the wiki in the referer string, like gl.wikipedia. Returns null if the referer isn't a wiki.",
        extended = "argument 0 is the url to analyze")
public class GetRefererWikiUDF extends UDF {

    public String evaluate(String referer) {
        if (referer == null || referer.equals("-") || referer.isEmpty()) return null;

        referer = referer.replaceFirst("http[s]?://", "");

        String project = Webrequest.getProjectFromHost(referer);
        if (Webrequest.getInstance().isWMFHostname(project + ".org")) {
            return project;
        } else {
            return null;
        }
    }
}
