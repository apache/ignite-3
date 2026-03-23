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

package org.apache.ignite.internal.rest.deployment;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.deployunit.DeploymentUnit;

/**
 * Interface for collecting input streams and converting them into deployment units.
 * 
 * <p>This interface provides a contract for accumulating input streams from various sources
 * (such as file uploads, network transfers, or other data sources) and organizing them into
 * a structured deployment unit that can be processed by the Apache Ignite deployment system.
 * 
 * <p>Implementations of this interface are typically used in REST endpoints, file upload
 * handlers, and other scenarios where deployment content is received in stream format and
 * needs to be converted into deployment units for processing.
 */
public interface InputStreamCollector {
    /**
     * Adds an input stream with the specified filename to the collection.
     * 
     * <p>This method accepts an input stream representing a file or resource that should be
     * included in the deployment unit. The filename parameter provides the logical name or
     * path for the content within the deployment unit structure.
     * 
     * <p>Once added, the input stream becomes managed by this collector and should not be
     * closed directly by the caller. The collector is responsible for proper cleanup of
     * all managed streams.
     *
     *
     * @param filename the logical name or path for the content within the deployment unit;
     *                must not be {@code null} or empty.
     * @param is the input stream containing the content to be added; must not be {@code null}.
     */
    void addInputStream(String filename, InputStream is);

    /**
     * Converts the collected input streams into a deployment unit.
     *
     * <p>This method creates a {@link DeploymentUnit} instance containing all the input streams
     * that have been added to this collector. The specific type of deployment unit returned depends on the implementation and the
     * characteristics of the collected content.
     *
     * @return a deployment unit future which will complete when all the input streams are collected input streams; never {@code null}.
     */
    CompletableFuture<DeploymentUnit> toDeploymentUnit();

    void rollback() throws Exception;
}
