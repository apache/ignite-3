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

package org.apache.ignite.internal.network.serialization;

import java.io.Serializable;
import java.util.Objects;

/** Serializable class for marshallable test. */
public class SimpleSerializableObject implements Serializable {
    private static final long serialVersionUID = 0L;

    private final int val;

    public SimpleSerializableObject(int val) {
        this.val = val;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleSerializableObject simpleSerializableObject = (SimpleSerializableObject) o;
        return val == simpleSerializableObject.val;
    }

    @Override
    public int hashCode() {
        return Objects.hash(val);
    }
}
