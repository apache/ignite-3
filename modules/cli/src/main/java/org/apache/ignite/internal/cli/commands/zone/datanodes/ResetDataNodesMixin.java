/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.commands.zone.datanodes;

import static org.apache.ignite.internal.cli.commands.Options.Constants.RESET_DATA_NODES_ZONE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RESET_DATA_NODES_ZONE_NAMES_OPTION_DESC;

import java.util.List;
import picocli.CommandLine.Option;

/** Arguments for reset data nodes command. */
public class ResetDataNodesMixin {
    @Option(
            names = RESET_DATA_NODES_ZONE_NAMES_OPTION,
            description = RESET_DATA_NODES_ZONE_NAMES_OPTION_DESC,
            split = ","
    )
    private List<String> zoneNames;

    /** Returns names of zones to reset data nodes for. */
    public List<String> zoneNames() {
        return zoneNames;
    }
}
