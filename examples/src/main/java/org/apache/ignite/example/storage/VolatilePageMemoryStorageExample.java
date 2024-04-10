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

package org.apache.ignite.example.storage;

/**
 * This example demonstrates a usage of the PageMemory storage engine configured with an in-memory data region.
 *
 * <p>To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into your IDE.</li>
 *     <li>
 *         Download and prepare artifacts for running an Ignite node using the CLI tool (if not done yet):<br>
 *         {@code ignite bootstrap}
 *     </li>
 *     <li>
 *         Start an Ignite node using the CLI tool:<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.conf my-first-node}
 *     </li>
 *     <li>
 *         Cluster initialization using the CLI tool (if not done yet):<br>
 *         {@code ignite cluster init --cluster-name=ignite-cluster --node-endpoint=localhost:10300 --meta-storage-node=my-first-node}
 *     </li>
 *     <li>
 *         Add configuration for an in-memory data region of the PageMemory storage engine using the CLI tool (if not done yet):<br>
 *         {@code ignite cluster config update "aimem.regions.in-memory"}
 *     </li>
 *     <li>Run the example in the IDE.</li>
 *     <li>
 *         Stop the Ignite node using the CLI tool:<br>
 *         {@code ignite node stop my-first-node}
 *     </li>
 * </ol>
 */
public class VolatilePageMemoryStorageExample {
    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        new StorageEngineExample("in-memory").run();
    }
}
