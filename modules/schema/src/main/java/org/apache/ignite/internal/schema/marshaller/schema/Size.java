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

package org.apache.ignite.internal.schema.marshaller.schema;

/**
 *
 */
public final class Size {
    /** String array length. */
    public static final int STRING_HEADER = 4;

    /** Array length. */
    public static final int ARRAY_HEADER_LENGTH = 4;

    /** Boolean. */
    public static final int BOOL = 1;

    /** Byte. */
    public static final int BYTE = 1;

    /** Short. */
    public static final int SHORT = 2;

    /** Int. */
    public static final int INT = 4;

    private Size() {

    }
}
