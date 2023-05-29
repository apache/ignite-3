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

import java.util.List;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.util.KeyNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Validation context implementation.
 */
class ValidationContextImpl<VIEWT> implements ValidationContext<VIEWT> {
    /** Cached storage roots with the current version of data. */
    private final SuperRoot oldRoots;

    /** Updated values that need to be validated. */
    private final SuperRoot newRoots;

    /**
     * Current node/configuration value.
     *
     * @see #getNewValue()
     */
    private final VIEWT val;

    /** Key corresponding to the value. */
    private final String currentKey;

    /** List representation of {@link #currentKey}. */
    private final List<String> currentPath;

    /** List of issues, should be used as a write-only collection. */
    private final List<ValidationIssue> issues;

    /**
     * Constructor.
     *
     * @param oldRoots Old roots.
     * @param newRoots New roots.
     * @param val New value of currently validated configuration.
     * @param currentKey Key corresponding to the value.
     * @param currentPath List representation of {@code currentKey}.
     * @param issues List of issues, should be used as a write-only collection.
     */
    ValidationContextImpl(
            SuperRoot oldRoots,
            SuperRoot newRoots,
            VIEWT val,
            String currentKey,
            List<String> currentPath,
            List<ValidationIssue> issues
    ) {
        this.oldRoots = oldRoots;
        this.newRoots = newRoots;
        this.val = val;
        this.currentKey = currentKey;
        this.currentPath = currentPath;
        this.issues = issues;

        assert !currentPath.isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public String currentKey() {
        return currentKey;
    }

    /** {@inheritDoc} */
    @Override
    public VIEWT getOldValue() {
        try {
            return ConfigurationUtil.<VIEWT>find(currentPath, oldRoots, true).value();
        } catch (KeyNotFoundException ignore) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public VIEWT getNewValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override
    public <ROOT> ROOT getOldRoot(RootKey<?, ROOT> rootKey) {
        return (ROOT) oldRoots.getRoot(rootKey);
    }

    /** {@inheritDoc} */
    @Override
    public <ROOT> ROOT getNewRoot(RootKey<?, ROOT> rootKey) {
        return (ROOT) newRoots.getRoot(rootKey);
    }

    /** {@inheritDoc} */
    @Override
    public void addIssue(ValidationIssue issue) {
        issues.add(issue);
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T getOldOwner() {
        return findOwner(oldRoots);
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T getNewOwner() {
        return findOwner(newRoots);
    }

    private <T> @Nullable T findOwner(SuperRoot superRoot) {
        try {
            return currentPath.size() <= 1
                    ? null
                    : ConfigurationUtil.<T>find(currentPath.subList(0, currentPath.size() - 1), superRoot, true).value();
        } catch (KeyNotFoundException ignore) {
            return null;
        }
    }
}
