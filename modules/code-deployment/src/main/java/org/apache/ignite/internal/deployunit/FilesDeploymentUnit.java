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

package org.apache.ignite.internal.deployunit;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link DeploymentUnit} that handles regular deployment content in local FS path.
 */
public class FilesDeploymentUnit implements DeploymentUnit {
    private final Map<String, Path> content;

    /**
     * Constructor.
     */
    public FilesDeploymentUnit(Map<String, Path> content) {
        this.content = content;
    }

    /**
     * Returns the deployment unit content as a map of file names to input streams.
     */
    public Map<String, Path> content() {
        return content;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public <T, R> CompletableFuture<R> process(DeploymentUnitProcessor<T, R> processor, T arg) {
        return processor.processFilesContent(this, arg);
    }
}
