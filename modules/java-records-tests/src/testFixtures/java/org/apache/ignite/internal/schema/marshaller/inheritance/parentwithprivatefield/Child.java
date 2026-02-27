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

package org.apache.ignite.internal.schema.marshaller.inheritance.parentwithprivatefield;

import java.util.Objects;
import org.apache.ignite.catalog.annotations.Column;

/** Test class. */
public class Child extends Parent {
    @Column("val2")
    String val2;

    private Child() {
    }

    public Child(Integer key, String val, String val2) {
        super(key, val);
        this.val2 = val2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Child that = (Child) o;
        return Objects.equals(getKey(), that.getKey())
                && Objects.equals(this.getVal(), that.getVal())
                && Objects.equals(this.val2, that.val2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKey(), getVal(), val2);
    }
}
