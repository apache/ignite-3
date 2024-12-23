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

package org.apache.ignite.internal.sql.engine.message;

/** Thrown by {@link MessageService} when recipient cannot be found in physical topology. */
public class UnknownNodeException extends RuntimeException {
    private static final long serialVersionUID = -8242883657846080305L;

    private final String nodeName;

    /** Constructs the object. */ 
    UnknownNodeException(String nodeName) {
        super("Unknown node: " + nodeName, null, true, false);

        this.nodeName = nodeName;
    }

    /** Returns name of the node that cannot be found in physical topology. */
    public String nodeName() {
        return nodeName;
    }
}
