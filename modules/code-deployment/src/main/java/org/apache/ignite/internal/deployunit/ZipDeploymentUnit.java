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

import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipInputStream;

/**
 * A specialized implementation of {@link DeploymentUnit} that handles ZIP-compressed deployment content.
 * 
 * <p>This class represents a deployment unit that contains ZIP-compressed archive that require extraction during processing.
 */
public class ZipDeploymentUnit implements DeploymentUnit {
    private final ZipInputStream zis;

    /**
     * Constructor.
     */
    public ZipDeploymentUnit(ZipInputStream zis) {
        this.zis = zis;
    }

    /**
     * Processes the deployment unit zip content.
     * 
     * <p>This method implements the {@link DeploymentUnit} processing contract with specialized 
     * handling for ZIP content.
     *
     * @param <T> the type of argument passed to the processor.
     * @param processor the processor that will handle both regular and ZIP content;
     *                 must implement both {@code processContent} and {@code processContentWithUnzip} methods.
     * @param arg the argument to be passed to the processor during both processing phases.
     */
    @Override
    public <T, R> CompletableFuture<R> process(DeploymentUnitProcessor<T, R> processor, T arg) {
        return processor.processContentWithUnzip(this, arg);
    }

    /**
     * Returns the ZIP input streams that require extraction during processing.
     */
    public ZipInputStream zis() {
        return zis;
    }

    @Override
    public void close() throws Exception {
        zis.close();
    }
}
