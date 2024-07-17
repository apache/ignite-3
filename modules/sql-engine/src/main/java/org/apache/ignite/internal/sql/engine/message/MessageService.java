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

package org.apache.ignite.internal.sql.engine.message;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.sql.engine.exec.LifecycleAware;

/**
 * MessageService interface.
 */
// TODO: Documentation https://issues.apache.org/jira/browse/IGNITE-15859
public interface MessageService extends LifecycleAware {
    /**
     * Sends a message to given node.
     *
     * @param nodeName Node consistent ID.
     * @param msg Message.
     */
    CompletableFuture<Void> send(String nodeName, NetworkMessage msg);

    /**
     * Registers a listener for messages of a given type.
     *
     * @param lsnr Listener.
     * @param msgId Message id.
     */
    void register(MessageListener lsnr, short msgId);
}
