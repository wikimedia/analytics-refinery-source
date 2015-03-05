// Copyright 2014 Wikimedia Foundation
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

package org.wikimedia.analytics.refinery.tools.guard;

import org.wikimedia.analytics.refinery.core.MediaFileUrlParser;
import org.wikimedia.analytics.refinery.core.MediaFileUrlInfo;
import org.wikimedia.analytics.refinery.core.MediaFileUrlInfo.Classification;

/**
 * Guard for parsing urls for media files in the upload domain.
 * <p>
 * This guard reads urls from stdin an passes them through MediaFileUrlParser.
 * Urls that MediaFileUrlParser cannot parse are considered a failure. So an
 * example input might be lines like:
 * <ul>
 *   <li>http://upload.wikimedia.org/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png</li>
 *   <li>/wikipedia/commons/thumb/a/ae/Essig-1.jpg/459px-Essig-1.jpg</li>
 *   <li>/wikipedia/commons/thumb/8/85/Defaut.svg/langfr-250px-Defaut.svg.png</li>
 * </ul>
 */
public class MediaFileUrlParserGuard extends StdinGuard {
    protected void check(String line) throws GuardException {
        MediaFileUrlInfo capsule =
                MediaFileUrlParser.parse(line);

        if (capsule == null) {
            throw new GuardException("Could not identify \"" + line + "\"");
        }
        if (capsule.getClassification() == Classification.UNKNOWN) {
            throw new GuardException("Could not identify \"" + line + "\"");
        }
    }

    public static void main(String[] args) {
        (new MediaFileUrlParserGuard()).doMain(args);
    }
}
