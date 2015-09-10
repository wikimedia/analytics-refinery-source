/**
 * Copyright (C) 2014  Wikimedia Foundation
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

package org.wikimedia.analytics.refinery.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.wikimedia.analytics.refinery.core.Webrequest;

/**
 * A hive UDF to identify WikimediaBot.
 */
public class IsWikimediaBotUDF extends UDF {
    public boolean evaluate(
        String user_agent
    ) {
        Webrequest webrequest_inst = Webrequest.getInstance();
        return webrequest_inst.isWikimediaBot(
                user_agent
        );
    }
}