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

package org.apache.ignite.internal.raft;

/**
 * Special type of exception used when a target peer is not present in the physical topology.
 *
 * <p>The stacktrace is omitted on purpose as to reduce log pollution (this exception is thrown and logged nearly immediately).
 */
public class PeerUnavailableException extends RuntimeException {
    public PeerUnavailableException(String consistentId) {
        super("Peer " + consistentId + " is unavailable", null, true, false);
    }
}
