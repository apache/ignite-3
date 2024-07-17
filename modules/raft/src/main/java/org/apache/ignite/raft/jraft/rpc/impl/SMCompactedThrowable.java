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

package org.apache.ignite.raft.jraft.rpc.impl;

import java.io.Serializable;

/**
 * Compacted version of throwable from client's state machine logic.
 * <p>
 * Note that this class does not preserve {@code traceId} and {@code code} from org.apache.ignite.lang.TraceableException,
 * so {@link SMThrowable} should be used instead.
 */
public class SMCompactedThrowable implements SMThrowable, Serializable {
    /** Throwable class name. */
    private final String clsName;

    /** Throwable message. */
    private final String msg;

    /**
     * @param th Throwable.
     */
    public SMCompactedThrowable(Throwable th) {
        this.clsName = th.getClass().getName();
        this.msg = th.getMessage();
    }

    /**
     * Returns class name of the original throwable.
     */
    public String throwableClassName() {
        return clsName;
    }

    /**
     * Returns message from the original throwable.
     */
    public String throwableMessage() {
        return msg;
    }
}
