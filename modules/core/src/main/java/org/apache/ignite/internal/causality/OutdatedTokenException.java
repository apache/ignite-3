/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.causality;

import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * This exception is thrown when {@link VersionedValue#get(long)} is called with an outdated token
 * (this means that the history size of VersionedValue is not enough in order to get a value related to the token).
 *
 * <p>{@link VersionedValue} stores a value per the causality token.
 * See {@link VersionedValue#get(long)}.
 */
public class OutdatedTokenException extends IgniteInternalCheckedException {

    /**
     * Constructor.
     *
     * @param token The tokent which is expired.
     */
    public OutdatedTokenException(long outdatedToken, long actualToken, int historySize) {
        super(IgniteStringFormatter.format("Token expired [token={}, actualToken={}, historySize={}]", outdatedToken,
                actualToken, historySize));
    }
}
