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

package org.apache.ignite.internal.network.netty;

/**
 * Constants related to message acknowledgement in the networking module.
 */
class MessageAcknowledgement {
    /**
     * The threshold for determining when acknowledgements should be sent synchronously.
     * When the number of unacknowledged messages exceeds this value,
     * acknowledgements will be sent synchronously to prevent excessive message buildup.
     */
    static final long SYNC_ACK_THRESHOLD = 10_000;

    /**
     * Delay in milliseconds before sending acknowledgement batch.
     * Allows batching multiple acknowledgements for better network efficiency.
     */
    static final long POSTPONE_ACK_MILLIS = 200;
}
