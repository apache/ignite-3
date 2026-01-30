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

package org.apache.ignite.internal.raft.client;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.raft.PeerUnavailableException;

/**
 * Utility methods for classifying RAFT errors.
 */
class RaftErrorUtils {
    /**
     * Returns {@code true} if the given throwable represents a recoverable error that can be retried.
     *
     * @param t Throwable to check.
     * @return {@code true} if recoverable.
     */
    static boolean recoverable(Throwable t) {
        t = unwrapCause(t);

        return t instanceof TimeoutException
                || t instanceof IOException
                || t instanceof PeerUnavailableException
                || t instanceof RecipientLeftException;
    }
}
