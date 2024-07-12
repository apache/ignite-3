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

package org.apache.ignite.internal.network.messages;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.jetbrains.annotations.Nullable;

/**
 * {@link NetworkMessage} implementation.
 */
@Transferable(TestMessageTypes.ALL_TYPES)
public interface AllTypesMessage extends NetworkMessage, Serializable {
    byte byteA();

    @Nullable Byte boxedByte();

    short shortB();

    @Nullable Short boxedShort();

    int intC();

    @Nullable Integer boxedInt();

    long longD();

    @Nullable Long boxedLong();

    float floatE();

    @Nullable Float boxedFloat();

    double doubleF();

    @Nullable Double boxedDouble();

    char charG();

    @Nullable Character boxedChar();

    boolean booleanH();

    @Nullable Boolean boxedBoolean();

    byte @Nullable [] byteArrI();

    short @Nullable [] shortArrJ();

    int @Nullable [] intArrK();

    long @Nullable [] longArrL();

    float @Nullable [] floatArrM();

    double @Nullable [] doubleArrN();

    char @Nullable [] charArrO();

    boolean @Nullable [] booleanArrP();

    @Nullable
    String strQ();

    @Nullable
    BitSet bitSetR();

    @Nullable
    UUID uuidS();

    @Nullable
    IgniteUuid igniteUuidT();

    @Nullable
    NetworkMessage netMsgU();

    NetworkMessage @Nullable [] netMsgArrV();

    @Nullable
    Collection<NetworkMessage> netMsgCollW();

    @Nullable
    Map<String, NetworkMessage> netMsgMapX();

    @Nullable
    List<NetworkMessage> netMsgListY();

    @Nullable
    Set<NetworkMessage> netMsgSetY();

    @Nullable
    ByteBuffer byteBufferZ();

    @IgniteToStringExclude
    @Nullable
    String excludedString();

    @IgniteToStringInclude(sensitive = true)
    @Nullable
    String sensitiveString();

    String [] stringArray();

    HybridTimestamp hybridTs();

    @Nullable HybridTimestamp hybridTsNullable();

    HybridTimestamp[] hybridTsArray();

    HybridTimestamp @Nullable [] hybridTsArrayNullable();

    List<HybridTimestamp> hybridTsList();

    @Nullable List<HybridTimestamp> hybridTsListNullable();

    Map<HybridTimestamp, HybridTimestamp> hybridTsMap();

    @Nullable Map<HybridTimestamp, HybridTimestamp> hybridTsMapNullable();
}
