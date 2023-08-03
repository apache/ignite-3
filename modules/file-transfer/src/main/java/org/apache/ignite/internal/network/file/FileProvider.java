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
import org.apache.ignite.internal.network.file.messages.Metadata;

/**
 * Files provider.
 */
public interface FileProvider<M extends Metadata> {

    /**
     * Returns files for the given metadata. The provider must return non empty list of files.
     *
     * @param metadata Metadata.
     * @return Files.
     */
    CompletableFuture<List<File>> files(M metadata);
}
