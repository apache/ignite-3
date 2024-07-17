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
import org.apache.ignite.internal.network.file.messages.Identifier;

/**
 * Consumes the uploaded files.
 */
public interface FileConsumer<I extends Identifier> {
    /**
     * Consumes the list of paths to the uploaded files. The paths are temporary and will be deleted after the method returns. If there is a
     * need to keep the files, they should be copied to a different location.
     *
     * @param identifier Uploaded files identifier.
     * @param uploadedFiles List of paths to the uploaded files.
     * @return Future that will be completed when the consumption is finished.
     */
    CompletableFuture<Void> consume(I identifier, List<Path> uploadedFiles);
}
