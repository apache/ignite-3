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

import org.apache.ignite.internal.network.ChannelType;

/**
 * Channel types used by the file transfer protocol.
 */
final class Channel {
    /**
     * File transfer channel.
     */
    static final ChannelType FILE_TRANSFER_CHANNEL = ChannelType.register((short) 3, "FileTransfer");

    private Channel() {
        // No-op.
    }
}
