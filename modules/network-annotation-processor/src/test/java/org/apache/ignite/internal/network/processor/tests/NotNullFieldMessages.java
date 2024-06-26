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

/** Messages with not {@link Nullable} fields. */
public class NotNullFieldMessages {
    /** Message with {@link UUID} field that is not {@link Nullable} field. */
    @Transferable(3)
    public interface NotNullArbitraryFieldMessage extends NetworkMessage {
        UUID value();
    }

    /** Message with {@link Marshallable} field that is not {@link Nullable}. */
    @Transferable(4)
    public interface NotNullMarshallableFieldMessage extends NetworkMessage {
        @Marshallable
        Object value();
    }

    /** Message with {@link NetworkMessage} field that is not {@link Nullable}. */
    @Transferable(5)
    public interface NotNullNetworkMessageFieldMessage extends NetworkMessage {
        NetworkMessage value();
    }

    /** Message with array field that is not {@link Nullable}. */
    // TODO: IGNITE-22583 тут
    @Transferable(6)
    public interface NotNullArrayFieldMessage extends NetworkMessage {
        int[] value();
    }
}
