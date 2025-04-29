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

package org.apache.ignite.internal.client.proto;

/**
 * Server op response flags utils (see {@link ServerOp}).
 */
public class ServerOpResponseFlags {
    /** Error flag. */
    private static final int ERROR_FLAG = 4;

    /**
     * Gets error flag value.
     *
     * @param flags Flags.
     * @return Whether error is present.
     */
    public static boolean getErrorFlag(int flags) {
        return (flags & ERROR_FLAG) == ERROR_FLAG;
    }
}
