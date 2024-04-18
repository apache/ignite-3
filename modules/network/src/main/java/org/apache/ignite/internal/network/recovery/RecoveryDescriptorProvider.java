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

import java.util.Collection;
import java.util.UUID;

/**
 * Interface that provides recovery descriptors for channels between two nodes.
 */
public interface RecoveryDescriptorProvider {
    /**
     * Returns a {@link RecoveryDescriptor}.
     *
     * @param consistentId Remote node consistent id.
     * @param launchId Remote node launch id.
     * @param connectionIndex Connection id.
     * @return Recovery descriptor.
     */
    RecoveryDescriptor getRecoveryDescriptor(String consistentId, UUID launchId, short connectionIndex);

    /**
     * Returns descriptors corresponding to the remote node with the given launch ID.
     *
     * @param launchId Launch ID to identify the node.
     * @return Descriptors by launch ID.
     */
    Collection<RecoveryDescriptor> getRecoveryDescriptorsByLaunchId(UUID launchId);

    /**
     * Returns all descriptors.
     */
    Collection<RecoveryDescriptor> getAllRecoveryDescriptors();
}
