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

package org.apache.ignite.internal.cluster.management;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;

/**
 * Contains local attributes that are collected during node startup and become visible
 * to all cluster nodes after the local node is added to the logical topology.
 *
 * @see LogicalNode#userAttributes()
 * @see LogicalNode#systemAttributes()
 */
public interface NodeAttributes {
    /**
     * Returns user defined attributes from a node's configuration.
     */
    Map<String, String> userAttributes();

    /**
     * Returns internal attributes provided by system components.
     */
    Map<String, String> systemAttributes();

    /**
     * List of storage profiles, which the node supports.
     */
    List<String> storageProfiles();
}
