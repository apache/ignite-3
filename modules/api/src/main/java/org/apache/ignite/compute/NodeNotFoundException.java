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

package org.apache.ignite.compute;

import static org.apache.ignite.lang.ErrorGroups.Compute.NODE_NOT_FOUND_ERR;

import java.util.Set;
import java.util.UUID;

/**
 * Thrown when compute component can't find the node to run the job on in the cluster.
 */
public class NodeNotFoundException extends ComputeException {
    public NodeNotFoundException(Set<String> nodeNames) {
        super(NODE_NOT_FOUND_ERR, "None of the specified nodes are present in the cluster: " + nodeNames);
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-20140
    public NodeNotFoundException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
