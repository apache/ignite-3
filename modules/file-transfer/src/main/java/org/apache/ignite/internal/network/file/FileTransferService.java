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

package org.apache.ignite.internal.network.file;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.file.messages.Identifier;

/**
 * File transferring service.
 */
public interface FileTransferService extends IgniteComponent {
    /**
     * Adds a file provider for the given identifier.
     *
     * @param identifier Files identifier.
     * @param provider Provider.
     */
    <M extends Identifier> void addFileProvider(
            Class<M> identifier,
            FileProvider<M> provider
    );

    /**
     * Adds a file handler for the given identifier.
     *
     * @param identifier Files identifier.
     * @param consumer Consumer.
     */
    <M extends Identifier> void addFileConsumer(Class<M> identifier, FileConsumer<M> consumer);

    /**
     * Downloads files for the given identifier from the given node.
     *
     * @param sourceNodeConsistentId consistent ID of a node.
     * @param identifier Files identifier.
     * @param targetDir Target directory. The directory will be created if it doesn't exist. If the directory exists, it will be
     *         cleaned up.
     * @return Future that will be completed when the download is finished. The future will contain a list of paths to the downloaded
     *         files.
     */
    CompletableFuture<List<Path>> download(String sourceNodeConsistentId, Identifier identifier, Path targetDir);

    /**
     * Uploads files for the given identifier to the given node.
     *
     * @param targetNodeConsistentId consistent ID of a node.
     * @param identifier Files identifier.
     * @return Future that will be completed when the upload is finished.
     */
    CompletableFuture<Void> upload(String targetNodeConsistentId, Identifier identifier);
}
