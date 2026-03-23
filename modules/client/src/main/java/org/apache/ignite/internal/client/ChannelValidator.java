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

package org.apache.ignite.internal.client;

import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.RetryPolicy;

/**
 * Validator of the connection channel.
 *
 * <p>Performs protocol context validation when a connection is established.
 */
@FunctionalInterface
public interface ChannelValidator {
    /**
     * Validates connection channel when a connection to a node is established,
     *
     * <p>Any exception thrown from this method indicates that validation failed
     * and the connection channel to this node will be closed. However, if, after
     * an unsuccessful validation, it is necessary to retry connections according
     * to the configured {@link RetryPolicy}, then the exception must be an
     * instance of {@link IgniteClientConnectionException}.
     *
     * <p>If {@link RetryPolicy} is not specified, then after the first unsuccessful validation
     * the client will be closed with an exception received from the validator.
     *
     * <p>If {@link RetryPolicy} is specified, the client will attempt to reconnect to cluster
     * nodes according to the configured policy. If the connection is not established within
     * the policy-defined number of attempts due to channel validation, the client will be closed
     * with an exception received from the validator.
     *
     * @param ctx Protocol context.
     */
    void validate(ProtocolContext ctx);
}
