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


package org.apache.ignite.network.scalecube;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.network.AckResponse;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.Request;

/** */
class TestMessage extends Request<AckResponse> implements Serializable {
    /** Visible type for tests. */
    public static final short TYPE = 3;

    /** */
    private final String msg;

    /** */
    TestMessage(String msg) {
        this.msg = msg;
    }

    public String msg() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TestMessage message = (TestMessage)o;
        return Objects.equals(msg, message.msg);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(msg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TestMessage{" +
            "msg='" + msg + '\'' +
            '}';
    }

    /** {@inheritDoc} */
    @Override public short type() {
        return TYPE;
    }
}
