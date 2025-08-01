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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.configuration.validation.TestValidationUtil;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class MulticastAddressValidatorTest extends BaseIgniteAbstractTest {
    @Test
    public void notMulticastAddressValidationFails() {
        validate("128.0.0.1", "Multicast group is not multicast");
    }

    @Test
    public void notValidAddressValidationFails() {
        validate("256.0.0.0", "Multicast group is not a valid address");
    }

    @Test
    public void validMulticastAddressNoErrorsFound() {
        validate("228.5.6.7");
    }

    private static void validate(String config, String ... errorMessagePrefixes) {
        TestValidationUtil.validate(
                MulticastAddressValidator.INSTANCE,
                mock(MulticastAddress.class),
                mockValidationContext(null, config),
                errorMessagePrefixes
        );
    }
}
