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

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.util.ExceptionUtils;

/**
 * File transfer error.
 */
@Transferable(FileTransferMessageType.FILE_TRANSFER_ERROR)
public interface FileTransferError extends NetworkMessage {
    /**
     * Returns the error code.
     *
     * @return Error code.
     */
    int code();

    /**
     * Returns the error message.
     *
     * @return Error message.
     */
    String message();

    /**
     * Creates a new {@link FileTransferError} instance from the given {@link Throwable}.
     *
     * @param factory File transfer factory.
     * @param throwable Throwable.
     * @return File transfer error.
     */
    static FileTransferError fromThrowable(FileTransferFactory factory, Throwable throwable) {
        return factory.fileTransferError()
                .message(ExceptionUtils.unwrapCause(throwable).getMessage())
                .build();
    }

    /**
     * Converts the given {@link FileTransferError} to an {@link Exception}.
     *
     * @param error File transfer error.
     * @return Exception.
     */
    static Exception toException(FileTransferError error) {
        return new FileTransferException(error.message());
    }
}
