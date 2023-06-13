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

package org.apache.ignite.internal.configuration.validation;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;

/**
 * Implementation of {@link ConfigurationValidator} to be used in tests.
 * Always returns empty list.
 */
public class TestConfigurationValidator implements ConfigurationValidator {
    @Override
    public List<ValidationIssue> validateHocon(String cfg) {
        return Collections.emptyList();
    }

    @Override
    public List<ValidationIssue> validate(ConfigurationSource src) {
        return Collections.emptyList();
    }

    @Override
    public List<ValidationIssue> validate(SuperRoot newRoots) {
        return Collections.emptyList();
    }

    @Override
    public List<ValidationIssue> validate(SuperRoot oldRoots, SuperRoot newRoots) {
        return Collections.emptyList();
    }
}
