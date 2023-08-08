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

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * File transfer message types.
 */
@MessageGroup(groupType = 200, groupName = "FileTransfer")
public final class FileTransferMessageType {
    public static final short FILE_IDENTIFIER = 0;
    public static final short FILE_HEADER = 1;
    public static final short FILE_CHUNK = 2;
    public static final short FILE_TRANSFER_ERROR = 3;
    public static final short FILE_DOWNLOAD_REQUEST = 4;
    public static final short FILE_DOWNLOAD_RESPONSE = 5;
    public static final short FILE_UPLOAD_REQUEST = 6;
    public static final short FILE_UPLOAD_RESPONSE = 7;
    public static final short FILE_TRANSFER_ERROR_MESSAGE = 8;

    /**
     * File transferring metadata.
     */
    public static final class Identifier {
    }
}
