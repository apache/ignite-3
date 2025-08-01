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

import org.apache.ignite.internal.lang.ComponentStoppingException;
import org.apache.ignite.internal.lang.NodeStoppingException;

/**
 * Shortcut methods used to create {@link ExceptionFactory} instances to indicate that component cannot service requests anymore
 * as it (or the node) is being stopped.
 */
public class StoppingExceptionFactories {
    private static final ExceptionFactory INDICATE_COMPONENT_STOP = new ComponentStoppingExceptionFactory();

    private static final ExceptionFactory INDICATE_NODE_STOP = new NodeStoppingExceptionFactory();

    /**
     * Returns a factory indicating component stop (using {@link ComponentStoppingException}).
     */
    public static ExceptionFactory indicateComponentStop() {
        return INDICATE_COMPONENT_STOP;
    }

    /**
     * Returns a factory indicating whole node stop (using {@link NodeStoppingException}).
     */
    public static ExceptionFactory indicateNodeStop() {
        return INDICATE_NODE_STOP;
    }
}
