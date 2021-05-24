/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.app.IgnitionImpl;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Sample application integrating new configuration module and providing standard REST API to access and modify it.
 */
public class IgniteCliRunner {
    /**
     * Logger.
     */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteCliRunner.class);

    /**
     * Main entry point for run Ignite node.
     *
     * @param args CLI args to start new node.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1)
            throw new IllegalArgumentException("IgniteRunner must be executed with node name as a first args" +
                "and config path as a second arg.");

        var name = args[0];

        String jsonCfgStr = null;
        if (args.length == 2)
            jsonCfgStr = Files.readString(Path.of(args[1]));

        var ignition = new IgnitionImpl();

        ignition.start(name, jsonCfgStr);
    }
}
