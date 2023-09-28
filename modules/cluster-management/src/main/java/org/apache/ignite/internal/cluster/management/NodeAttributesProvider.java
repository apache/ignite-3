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

import java.util.Map;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;

/**
 * Local node attributes provider.
 */
public interface NodeAttributesProvider {
    /**
     * Returns a collection of internal local node attributes, that should be visible to other
     * cluster nodes in logical topology using {@link LogicalNode#systemAttributes()} method.
     *
     * <p>Attributes must be prepared when the component is initialized and/or started. That is, before the
     * local node finishes joining the cluster. Otherwise they will not be visible in the logical topology.
     *
     * @return Collection of attributes.
     */
    Map<String, String> nodeAttributes();
}
