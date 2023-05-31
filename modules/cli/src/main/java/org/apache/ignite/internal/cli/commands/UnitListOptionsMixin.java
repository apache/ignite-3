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

package org.apache.ignite.internal.cli.commands;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.cli.call.unit.ListUnitCallInput;
import org.apache.ignite.rest.client.model.DeploymentStatus;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Mixin for unit list commands.
 */
public class UnitListOptionsMixin {
    @ArgGroup(exclusive = false)
    private UnitVersion unitVersion;

    private static class UnitVersion {
        @Parameters(description = "Deployment unit id", arity = "1", defaultValue = Option.NULL_VALUE)
        private String unitId;

        @Option(names = "--version", description = "Filters out deployment unit by version (exact match assumed)")
        private String version;
    }

    @Option(names = "--status", description = "Filters out deployment unit by status", split = ",")
    private DeploymentStatus[] statuses;

    /**
     * Builds the {@link ListUnitCallInput} from this mixin options and provided URL.
     *
     * @param url Endpoint URL.
     * @return {@link ListUnitCallInput} constructed from options.
     */
    public ListUnitCallInput toListUnitCallInput(String url) {
        String unitId = unitVersion != null ? unitVersion.unitId : null;
        String version = unitVersion != null ? unitVersion.version : null;
        List<DeploymentStatus> statuses = this.statuses != null ? Arrays.asList(this.statuses) : null;

        return ListUnitCallInput.builder()
                .unitId(unitId)
                .version(version)
                .statuses(statuses)
                .url(url)
                .build();
    }
}
