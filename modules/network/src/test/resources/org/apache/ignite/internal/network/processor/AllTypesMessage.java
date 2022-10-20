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

package org.apache.ignite.internal.network.processor;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * {@link NetworkMessage} implementation.
 */
@Transferable(1)
public interface AllTypesMessage extends NetworkMessage, Serializable {
    byte byteA();

    short shortB();

    int intC();

    long longD();

    float floatE();

    double doubleF();

    char charG();

    boolean booleanH();

    byte[] byteArrI();

    short[] shortArrJ();

    int[] intArrK();

    long[] longArrL();

    float[] floatArrM();

    double[] doubleArrN();

    char[] charArrO();

    boolean[] booleanArrP();

    String strQ();

    BitSet bitSetR();

    UUID uuidS();

    IgniteUuid igniteUuidT();

    NetworkMessage netMsgU();

    NetworkMessage[] netMsgArrV();

    Collection<NetworkMessage> netMsgCollW();

    Map<String, NetworkMessage> netMsgMapX();
}
