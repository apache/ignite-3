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

import java.util.UUID;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * File transfer error message.
 */
@Transferable(FileTransferMessageType.FILE_TRANSFER_ERROR_MESSAGE)
public interface FileTransferErrorMessage extends NetworkMessage {
    /**
     * Returns transfer ID.
     *
     * @return Transfer ID.
     */
    UUID transferId();

    /**
     * Returns error message.
     *
     * @return Error message.
     */
    FileTransferError error();
}
