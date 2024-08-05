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

package org.apache.ignite.internal.security.authentication.basic;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BasicUserBuilderImpl implements BasicUserBuilder {
    private final String username;

    private final List<Consumer<BasicUserChange>> changes = new ArrayList<>();

    public BasicUserBuilderImpl(String username) {
        this.username = username;
    }

    public String username() {
        return username;
    }

    @Override
    public BasicUserBuilder setPassword(String password) {
        changes.add(change -> change.changePassword(password));
        return this;
    }

    public void change(BasicUserChange change) {
        changes.forEach(consumer -> consumer.accept(change));
    }
}
