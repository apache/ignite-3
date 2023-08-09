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

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferFactory;

/**
 * Utility class for file transfer messages.
 */
final class MessagesUtils {
    private MessagesUtils() {
        // No-op.
    }

    /**
     * Extracts file headers from files.
     *
     * @param factory File transfer factory.
     * @param paths List of paths.
     * @return List of file headers.
     */
    static List<FileHeader> getHeaders(FileTransferFactory factory, List<Path> paths) {
        return IntStream.range(0, paths.size())
                .mapToObj(id -> FileHeader.fromPath(factory, id, paths.get(id)))
                .collect(toList());
    }
}
