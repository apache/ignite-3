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

package org.apache.ignite.internal.network.recovery;

import java.util.UUID;

/**
 * Factory producing {@link RecoveryClientHandshakeManager} instances.
 */
public interface RecoveryClientHandshakeManagerFactory {
    /**
     * Produces a {@link RecoveryClientHandshakeManager} instance.
     *
     * @param launchId                   ID of the launch.
     * @param consistentId               Consistent ID of the node.
     * @param connectionId               ID of the connection.
     * @param recoveryDescriptorProvider Provider of recovery descriptors to be used.
     * @return Created manager.
     */
    RecoveryClientHandshakeManager create(
            UUID launchId,
            String consistentId,
            short connectionId,
            RecoveryDescriptorProvider recoveryDescriptorProvider
    );
}
