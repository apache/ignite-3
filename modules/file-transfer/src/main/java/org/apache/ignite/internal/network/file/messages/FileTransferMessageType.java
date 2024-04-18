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

import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * File transfer message types.
 */
@MessageGroup(groupType = 200, groupName = "FileTransfer")
public final class FileTransferMessageType {
    /**
     * Type for {@link Identifier}.
     */
    public static final short FILE_IDENTIFIER = 0;

    /**
     * Type for {@link FileHeader}.
     */
    public static final short FILE_HEADER = 1;

    /**
     * Type for {@link FileChunkMessage}.
     */
    public static final short FILE_CHUNK_MESSAGE = 2;

    /**
     * Type for {@link FileChunkResponse}.
     */
    public static final short FILE_CHUNK_RESPONSE = 3;

    /**
     * Type for {@link FileTransferError}.
     */
    public static final short FILE_TRANSFER_ERROR = 4;

    /**
     * Type for {@link FileDownloadRequest}.
     */
    public static final short FILE_DOWNLOAD_REQUEST = 5;

    /**
     * Type for {@link FileDownloadResponse}.
     */
    public static final short FILE_DOWNLOAD_RESPONSE = 6;

    /**
     * Type for {@link FileTransferInitMessage}.
     */
    public static final short FILE_TRANSFER_INIT_MESSAGE = 7;

    /**
     * Type for {@link FileTransferInitResponse}.
     */
    public static final short FILE_TRANSFER_INIT_RESPONSE = 8;

    /**
     * Type for {@link FileTransferErrorMessage}.
     */
    public static final short FILE_TRANSFER_ERROR_MESSAGE = 9;
}
