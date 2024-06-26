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

package org.apache.ignite.internal.network.processor.tests;

import java.util.UUID;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/** Messages with {@link Nullable} fields. */
public class NullableFieldMessages {
    /** Message with {@link UUID} field that is {@link Nullable} field. */
    @Transferable(7)
    public interface NullableArbitraryFieldMessage extends NetworkMessage {
        @Nullable
        UUID value();
    }

    /** Message with {@link Marshallable} field that is {@link Nullable}. */
    @Transferable(8)
    public interface NullableMarshallableFieldMessage extends NetworkMessage {
        @Nullable
        @Marshallable
        Object value();
    }

    /** Message with {@link NetworkMessage} field that is {@link Nullable}. */
    @Transferable(9)
    public interface NullableNetworkMessageFieldMessage extends NetworkMessage {
        @Nullable
        NetworkMessage value();
    }

    /** Message with array field that is {@link Nullable}. */
    @Transferable(10)
    public interface NullableArrayFieldMessage extends NetworkMessage {
        int @Nullable [] value();
    }
}
