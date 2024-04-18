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

package org.apache.ignite.internal.event;

/** {@link EventParameters} implementation that contains a Causality Token. */
public abstract class CausalEventParameters implements EventParameters {
    private final long causalityToken;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     */
    public CausalEventParameters(long causalityToken) {
        this.causalityToken = causalityToken;
    }

    /**
     * Returns a causality token.
     * The token is required for represent a causality dependency between several events.
     * The earlier the event occurred, the lower the value of the token.
     */
    public long causalityToken() {
        return causalityToken;
    }
}
