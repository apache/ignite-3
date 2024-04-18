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

package org.apache.ignite.internal.network;

import org.apache.ignite.lang.ErrorGroups.Network;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;

/**
 * Thrown when consistent ID cannot be resolved to a {@link ClusterNode} instance (i.e. when
 * there is no node with such consistent ID in the physical topology).
 */
public class UnresolvableConsistentIdException extends IgniteException {
    public UnresolvableConsistentIdException(String msg) {
        super(Network.UNRESOLVABLE_CONSISTENT_ID_ERR, msg);
    }
}
