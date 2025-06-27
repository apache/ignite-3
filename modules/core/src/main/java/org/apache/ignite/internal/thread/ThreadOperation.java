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

package org.apache.ignite.internal.thread;

/**
 * Operation that a thread might be allowed or denied to execute.
 */
public enum ThreadOperation {
    /** Storage read. */
    STORAGE_READ,
    /** Storage write. */
    STORAGE_WRITE,
    /** Access TX State storage. */
    TX_STATE_STORAGE_ACCESS,
    /** Make a blocking wait (involving taking a lock or waiting on a conditional variable or waiting for time to pass. */
    WAIT,
    /** This permission allows tread process RAFT action request. */
    PROCESS_RAFT_REQ;

    /**
     * Empty list of operations denoting that no potentially blocking/time consuming operations are allowed
     * to be executed on a thread.
     */
    public static final ThreadOperation[] NOTHING_ALLOWED = new ThreadOperation[0];
}
