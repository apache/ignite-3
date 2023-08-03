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

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Chunked file.
 */
@Transferable(FileTransferMessageType.FILE_CHUNK)
public interface FileChunkMessage extends NetworkMessage {
    Comparator<FileChunkMessage> COMPARATOR = Comparator.comparingLong(FileChunkMessage::offset);

    /**
     * Returns transfer ID.
     *
     * @return Transfer ID.
     */
    UUID transferId();

    /**
     * Returns file name.
     *
     * @return File name.
     */
    String fileName();

    /**
     * Returns offset.
     *
     * @return Offset.
     */
    long offset();

    /**
     * Returns data.
     *
     * @return Data.
     */
    byte[] data();
}
