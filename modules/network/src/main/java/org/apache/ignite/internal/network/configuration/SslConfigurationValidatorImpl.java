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

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Ssl configuration validator implementation.
 */
public class SslConfigurationValidatorImpl implements Validator<SslConfigurationValidator, SslView> {

    public static final SslConfigurationValidatorImpl INSTANCE = new SslConfigurationValidatorImpl();

    @Override
    public void validate(SslConfigurationValidator annotation, ValidationContext<SslView> ctx) {
        SslView ssl = ctx.getNewValue();
        if (ssl.enabled()) {
            if (ssl.keyStore() == null && ssl.trustStore() == null) {
                ctx.addIssue(new ValidationIssue(
                        ctx.currentKey(),
                        "At least one of keyStore or trustStore must be specified"
                ));
            }
        }
    }
}
