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

package org.apache.ignite.internal.network.annotations;

/**
 * Special interface that must be present for {@link Enum} that will be transferred in a network message (with {@link Transferable}).
 *
 * <p>This interface serves to protect against situations such as adding new constants to enumeration in the middle and errors occurring
 * during serialization and deserialization of messages.</p>
 *
 * <p>It is recommended that when adding new constants to enumeration, use a new {@link #transferableId()} and do not delete old constants
 * to prevent errors during serialization and deserialization. Which can occur, for example, when recovering a node that saved commands to
 * the replication log.</p>
 */
public interface TransferableEnum {
    /** Non-negative and unique constant ID in the current enumeration to transfer in a network message. */
    int transferableId();
}
