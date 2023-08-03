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

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.file.messages.TransferMetadata;

/**
 * Handler for the uploaded file.
 */
public interface FileHandler<M extends TransferMetadata> {
    /**
     * Handles the uploaded files.
     *
     * @param uploadedFiles The temporary path to the directory with the uploaded files. The directory will be deleted after the
     *         method returns.
     * @return A future that will be completed when the file is handled.
     */
    CompletableFuture<Void> handleUpload(M metadata, List<File> uploadedFiles);
}
