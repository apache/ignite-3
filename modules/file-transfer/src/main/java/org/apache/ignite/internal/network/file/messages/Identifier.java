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

import org.apache.ignite.internal.network.file.FileConsumer;
import org.apache.ignite.internal.network.file.FileProvider;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Files identifier. This interface is used to mark all identifier messages. Identifier messages are used to retrieve files from
 * {@link FileProvider} and handle them on the receiving side by {@link FileConsumer}.
 */
@Transferable(FileTransferMessageType.FILE_IDENTIFIER)
public interface Identifier extends NetworkMessage {
}
