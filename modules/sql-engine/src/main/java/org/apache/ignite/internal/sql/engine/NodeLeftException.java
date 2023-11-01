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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.lang.ErrorGroups.Common.NODE_LEFT_ERR;

import java.util.UUID;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * The exception is thrown when SQL engine can not process an operation because a node has a left cluster.
 */
public class NodeLeftException extends IgniteException {

    private static final long serialVersionUID = 0L;

    /** Constructor. */
    public NodeLeftException(String nodeName) {
        super(NODE_LEFT_ERR, "Node left the cluster. Node: " + nodeName);
    }

    /**
     * Constructor is used to copy the original exception with an extended error description.
     */
    public NodeLeftException(UUID traceId, int code, String message, @Nullable Throwable cause) {
        super(traceId, code, message, cause);

        assert code == NODE_LEFT_ERR;
    }
}
