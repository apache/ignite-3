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

package org.apache.ignite.internal.configuration;

import java.nio.file.Path;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.storage.LogStorageManager;

/**
 * A helper that creates an instance of {@link RaftGroupOptionsConfigurer}.
 */
public interface RaftGroupOptionsConfigHelper {

    /**
     * Creates an instance of {@link RaftGroupOptionsConfigurer} that can be used to configure Raft options.
     *
     * @param logStorageManager Log storage manager.
     * @param serverDataPath Server data path.
     * @return Raft options configurer.
     */
    static RaftGroupOptionsConfigurer configureProperties(LogStorageManager logStorageManager, Path serverDataPath) {
        return options -> {
            RaftGroupOptions groupOptions = (RaftGroupOptions) options;

            if (groupOptions.getLogStorageManager() == null) {
                groupOptions.setLogStorageManager(logStorageManager);
            }
            if (groupOptions.serverDataPath() == null) {
                groupOptions.serverDataPath(serverDataPath);
            }
        };
    }
}
