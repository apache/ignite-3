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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.UUID;
import org.apache.ignite.internal.lang.RemoteException;
import org.jetbrains.annotations.Nullable;

/**
 * Remote query fragment exception. This exception is used to indicate a remote fragment execution has been failed.
 */
public class RemoteFragmentExecutionException extends RemoteException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    private final String nodeName;

    private final UUID queryId;

    private final long fragmentId;

    /**
     * Constructor.
     *
     * @param nodeName Node consistent ID.
     * @param queryId Query ID.
     * @param fragmentId Fragment ID.
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Original error message from remote node.
     */
    public RemoteFragmentExecutionException(
            String nodeName,
            UUID queryId,
            long fragmentId,
            UUID traceId,
            int code,
            @Nullable String message
    ) {
        super(traceId, code, message);

        this.nodeName = nodeName;
        this.queryId = queryId;
        this.fragmentId = fragmentId;
    }

    /**
     * Get node consistent ID.
     */
    public String nodeName() {
        return nodeName;
    }

    /**
     * Get query ID.
     */
    public UUID queryId() {
        return queryId;
    }

    /**
     * Get fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }
}
