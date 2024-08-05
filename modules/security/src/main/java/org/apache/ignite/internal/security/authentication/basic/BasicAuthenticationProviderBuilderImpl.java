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
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderBuilderImpl;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderChange;
import org.apache.ignite.security.authentication.basic.BasicAuthenticationProviderBuilder;
import org.apache.ignite.security.authentication.basic.BasicUserBuilder;

public class BasicAuthenticationProviderBuilderImpl extends AuthenticationProviderBuilderImpl implements
        BasicAuthenticationProviderBuilder {
    private final List<BasicUserBuilderImpl> users = new ArrayList<>();

    public BasicAuthenticationProviderBuilderImpl(String name) {
        super(name);
    }

    @Override
    public BasicAuthenticationProviderBuilder addUser(BasicUserBuilder user) {
        users.add(((BasicUserBuilderImpl) user));
        return this;
    }

    @Override
    public void change(AuthenticationProviderChange authenticationProviderChange) {
        authenticationProviderChange.convert(BasicAuthenticationProviderChange.class)
                .changeUsers(usersChange -> users.forEach(user -> usersChange.create(user.username(), user::change)));
    }
}
