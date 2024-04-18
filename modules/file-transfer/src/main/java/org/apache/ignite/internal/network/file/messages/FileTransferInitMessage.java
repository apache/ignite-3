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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * File transfer init message. This message is sent by the sender to the receiver to initiate a file transfer.
 */
@Transferable(FileTransferMessageType.FILE_TRANSFER_INIT_MESSAGE)
public interface FileTransferInitMessage extends NetworkMessage {
    /**
     * Returns the identifier of the file transfer.
     *
     * @return Identifier of the file transfer.
     */
    UUID transferId();

    /**
     * Returns the identifier of the files that are going to be transferred.
     *
     * @return Identifier of the files.
     */
    Identifier identifier();

    /**
     * Returns the headers of the files that are going to be transferred.
     *
     * @return Headers of the files.
     */
    List<FileHeader> headers();
}
