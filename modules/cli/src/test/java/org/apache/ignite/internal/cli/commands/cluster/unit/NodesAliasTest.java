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

package org.apache.ignite.internal.cli.commands.cluster.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.rest.client.model.DeployMode;
import org.junit.jupiter.api.Test;

class NodesAliasTest {

    @Test
    void hasAllValuesOfDeployMode() {
        Set<String> deployModes = Arrays.stream(DeployMode.values()).map(Enum::name).collect(Collectors.toSet());
        Set<String> nodeAliases = Arrays.stream(NodesAlias.values()).map(Enum::name).collect(Collectors.toSet());
        assertEquals(deployModes, nodeAliases);
    }
}
