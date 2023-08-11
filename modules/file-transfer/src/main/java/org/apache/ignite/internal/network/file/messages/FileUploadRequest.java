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
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * File upload request. This message is sent by the sender to the receiver to request a file upload.
 */
@Transferable(FileTransferMessageType.FILE_UPLOAD_REQUEST)
public interface FileUploadRequest extends NetworkMessage {
    /**
     * Returns the identifier of the files that are going to be uploaded.
     *
     * @return Identifier of the files.
     */
    Identifier identifier();

    /**
     * Returns the headers of the files that are going to be uploaded.
     *
     * @return Headers of the files.
     */
    List<FileHeader> headers();
}
