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

package org.apache.ignite.internal.cli.config;

import java.io.File;
import java.nio.file.Path;

/**
 * Helper class to access to state folder of CLI.
 */
public final class StateFolderProvider {
    private static final String XDG_STATE_HOME = "XDG_STATE_HOME";
    private static final String PARENT_FOLDER_NAME = "ignitecli";

    private StateFolderProvider() {
    }

    /**
     * Gets the path for the state.
     *
     * @return Folder for state storage.
     */
    public static File getStateFile(String name) {
        return getStateRoot().resolve(PARENT_FOLDER_NAME).resolve(name).toFile();
    }

    private static Path getStateRoot() {
        String xdgStateHome = System.getenv(XDG_STATE_HOME);
        if (xdgStateHome != null) {
            return Path.of(xdgStateHome);
        } else {
            return Path.of(System.getProperty("user.home"), ".local", "state");
        }
    }
}
