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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link DeploymentUnit} that handles regular deployment content in stream format.
 *
 * <p>This class represents a deployment unit containing a collection of files, where each file is
 * represented as a mapping from file name to its corresponding {@link InputStream}. This implementation is designed for straightforward
 * deployment scenarios where the content does not require compression or special extraction handling.
 */
public class StreamDeploymentUnit implements DeploymentUnit {
    /**
     * The deployment unit content represented as a mapping from file names to their input streams.
     */
    private final Map<String, InputStream> content;

    /**
     * Constructor.
     */
    public StreamDeploymentUnit(Map<String, InputStream> content) {
        this.content = content;
    }

    /**
     * Returns the deployment unit content as a map of file names to input streams.
     */
    public Map<String, InputStream> content() {
        return content;
    }

    @Override
    public void close() throws Exception {
        closeAll(content.values());
    }

    @Override
    public <T, R> CompletableFuture<R> process(DeploymentUnitProcessor<T, R> processor, T arg) {
        return processor.processStreamContent(this, arg);
    }
}
