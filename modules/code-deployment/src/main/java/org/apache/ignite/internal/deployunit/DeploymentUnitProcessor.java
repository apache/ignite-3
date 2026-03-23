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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Processor interface for handling deployment unit content operations.
 *
 * <p>This interface defines a contract for processing deployment units, providing methods to handle
 * both regular deployment units and ZIP-based deployment units. Implementations of this interface
 * can perform various operations on deployment unit content such as deployment, validation, 
 * transformation, or extraction.
 *
 * @param <T> the type of argument passed to the processing methods
 */
public interface DeploymentUnitProcessor<T, R> {
    /**
     * Processes the content of a regular deployment unit.
     *
     * <p>This method handles deployment units that contain a collection of files represented
     * as input streams. The implementation should process each file in the deployment unit
     * according to the specific processor's logic.
     *
     * @param unit the deployment unit containing the content to be processed
     * @param arg the argument to be used during processing
     * @throws IOException if an I/O error occurs during processing
     */
    CompletableFuture<R> processFilesContent(FilesDeploymentUnit unit, T arg);

    CompletableFuture<R> processStreamContent(StreamDeploymentUnit unit, T arg);

    /**
     * Processes the content of a ZIP-based deployment unit with automatic extraction.
     *
     * <p>This method handles deployment units that contain ZIP archives. The implementation
     * should process the ZIP content by extracting and handling each entry according to the
     * specific processor's logic. This method is typically used when the deployment unit
     * contains compressed content that needs to be extracted during processing.
     *
     * @param unit the ZIP deployment unit containing the compressed content to be processed
     * @param arg the argument to be used during processing
     * @throws IOException if an I/O error occurs during processing or extraction
     */
    CompletableFuture<R> processContentWithUnzip(ZipDeploymentUnit unit, T arg);
}
