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

package org.apache.ignite.internal.network.file.messages;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * File header. This message is sent by the sender to the receiver to provide information about the file to be transferred.
 */
@Transferable(FileTransferMessageType.FILE_HEADER)
public interface FileHeader extends NetworkMessage {
    /**
     * Returns the name of the file.
     *
     * @return File name.
     */
    String name();

    /**
     * Returns the size of the file in bytes.
     *
     * @return File size.
     */
    long length();

    /**
     * Creates a new {@link FileHeader} instance from the given {@link File}.
     */
    static FileHeader fromPath(FileTransferFactory factory, Path path) {
        return factory.fileHeader()
                .name(path.getFileName().toString())
                .length(path.toFile().length())
                .build();
    }

    /**
     * Creates a list of {@link FileHeader} instances from the given list of {@link Path}s.
     *
     * @param factory File transfer factory.
     * @param paths List of paths.
     * @return List of file headers.
     */
    static List<FileHeader> fromPaths(FileTransferFactory factory, List<Path> paths) {
        return paths.stream().map(path -> fromPath(factory, path))
                .collect(toList());
    }
}
