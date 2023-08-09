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

import java.io.File;
import java.nio.file.Path;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * File header.
 */
@Transferable(FileTransferMessageType.FILE_HEADER)
public interface FileHeader extends NetworkMessage {
    /**
     * Returns the id of the file.
     *
     * @return File id.
     */
    int id();

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
    static FileHeader fromPath(FileTransferFactory factory, int id, Path path) {
        return factory.fileHeader()
                .id(id)
                .name(path.getFileName().toString())
                .length(path.toFile().length())
                .build();
    }
}
