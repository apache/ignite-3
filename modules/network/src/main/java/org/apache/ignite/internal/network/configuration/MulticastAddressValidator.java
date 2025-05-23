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

package org.apache.ignite.internal.network.configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/** Validates that a value is a correct multicast address. */
public class MulticastAddressValidator implements Validator<MulticastAddress, String> {
    public static final MulticastAddressValidator INSTANCE = new MulticastAddressValidator();

    @Override
    public void validate(MulticastAddress annotation, ValidationContext<String> ctx) {
        String multicastGroup = ctx.getNewValue();
        try {
            InetAddress address = InetAddress.getByName(multicastGroup);
            if (!address.isMulticastAddress()) {
                ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Multicast group is not multicast"));
            }
        } catch (UnknownHostException e) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Multicast group is not a valid address"));
        }
    }
}
