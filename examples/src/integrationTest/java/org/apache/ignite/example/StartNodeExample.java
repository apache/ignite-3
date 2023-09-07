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

package org.apache.ignite.example;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;

/**
 * Tests for transactional examples.
 */
public class StartNodeExample {

    public static void main(String[] args) throws InterruptedException {
        String nodeName = args[0];
        if (nodeName == null || nodeName.isBlank()) {
            throw new IllegalArgumentException("nodeName is empty");
        }

        String configFileName = args[1];
        if (configFileName == null || configFileName.isBlank()) {
            throw new IllegalArgumentException("configFileName is empty");
        }

        boolean activate = Boolean.parseBoolean(args[2]);

        CompletableFuture<Ignite> igniteFuture = IgnitionManager.start(
                nodeName,
                Path.of("examples", "config", configFileName),
                Path.of("examples", "work", nodeName)
        );

        if (activate) {
            InitParameters initParameters = InitParameters.builder()
                    .destinationNodeName(nodeName)
                    .metaStorageNodeNames(List.of(/*"ignite-node-0",*/ nodeName))
                    .clusterName("cluster")
                    .build();

            IgnitionManager.init(initParameters);
        }

        // We can call without a timeout, since the future is guaranteed to be completed above.
        IgniteImpl ignite = (IgniteImpl) igniteFuture.join();

        TimeUnit.HOURS.sleep(3);
    }
}
