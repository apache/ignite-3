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

package org.apache.ignite.internal.table.distributed.disaster.exceptions;

import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.REQUEST_FORWARD_ERR;

/** Exception is thrown when forwarding a disaster recovery request to another node fails. */
public class DisasterRecoveryRequestForwardException extends DisasterRecoveryException {
    private static final long serialVersionUID = 3247863547012365789L;

    /**
     * Creates a new disaster recovery request forward exception.
     *
     * @param targetNodeName Name of the target node to which the request was forwarded.
     * @param errorMessage Error message received from the target node or describing the forward failure.
     */
    public DisasterRecoveryRequestForwardException(String targetNodeName, String errorMessage) {
        super(REQUEST_FORWARD_ERR, String.format("Failed to forward disaster recovery request to node %s,"
                        + " try calling the node directly. %s'",
                targetNodeName, errorMessage));
    }
}
