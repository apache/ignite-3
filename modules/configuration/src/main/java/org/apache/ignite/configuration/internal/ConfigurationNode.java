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

package org.apache.ignite.configuration.internal;

import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.internal.util.ConfigurationUtil;
import org.apache.ignite.configuration.internal.util.KeyNotFoundException;
import org.apache.ignite.configuration.tree.TraversableTreeNode;

/** */
public abstract class ConfigurationNode<VIEW> {
    protected final List<String> keys;

    protected final String key;

    protected final ConfigurationChanger changer;

    protected final RootKey<?> rootKey;

    private volatile TraversableTreeNode cachedRootNode;

    protected VIEW val;

    private boolean invalid;

    protected ConfigurationNode(List<String> prefix, String key, RootKey<?> rootKey, ConfigurationChanger changer) {
        this.keys = ConfigurationUtil.appendKey(prefix, key);
        this.key = key;
        this.rootKey = rootKey;
        this.changer = changer;
    }

    /** */
    protected VIEW refresh() {
        checkValueValidity();

        TraversableTreeNode newRootNode = changer.getRootNode(rootKey);
        TraversableTreeNode oldRootNode = cachedRootNode;

        if (oldRootNode == newRootNode)
            return val;

        try {
            VIEW newVal = (VIEW)ConfigurationUtil.find(keys.subList(1, keys.size()), newRootNode);

            synchronized (this) {
                if (cachedRootNode == oldRootNode) {
                    cachedRootNode = newRootNode;

                    refresh0(newVal);

                    return val = newVal;
                }
                else {
                    checkValueValidity();

                    return val;
                }
            }
        }
        catch (KeyNotFoundException e) {
            invalid = true;

            throw new NoSuchElementException(ConfigurationUtil.join(keys));
        }
    }

    protected abstract void refresh0(VIEW val);

    private void checkValueValidity() {
        if (invalid)
            throw new NoSuchElementException(ConfigurationUtil.join(keys));
    }

    /** */
    public final VIEW viewValue() {
        return refresh();
    }
}
