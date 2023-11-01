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

package org.apache.ignite.internal.deployunit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitReadException;

/**
 * Unit content representation.
 */
public class UnitContent implements Iterable<Entry<String, byte[]>> {
    private final Map<String, byte[]> files;

    public UnitContent(Map<String, byte[]> files) {
        this.files = files;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnitContent that = (UnitContent) o;

        if (files.size() != that.files.size()) {
            return false;
        }

        for (Entry<String, byte[]> e : files.entrySet()) {
            String key = e.getKey();
            byte[] value = e.getValue();
            if (value == null) {
                if (!(that.files.get(key) == null && that.files.containsKey(key))) {
                    return false;
                }
            } else if (!Arrays.equals(value, that.files.get(key))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return files.hashCode();
    }

    @Override
    public Iterator<Entry<String, byte[]>> iterator() {
        return files.entrySet().iterator();
    }

    /**
     * Read unit content from unit {@link DeploymentUnit}.
     *
     * @param deploymentUnit Deployment unit instance.
     * @return Unit content from provided deployment unit.
     */
    public static UnitContent readContent(DeploymentUnit deploymentUnit) {
        Map<String, byte[]> map = deploymentUnit.content().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> {
                    try {
                        return entry.getValue().readAllBytes();
                    } catch (IOException e) {
                        throw new DeploymentUnitReadException(e);
                    }
                }));
        return new UnitContent(map);
    }

    /**
     * Convert unit content to {@link DeploymentUnit}.
     *
     * @param content Unit content.
     * @return Deployment unit instance.
     */
    public static DeploymentUnit toDeploymentUnit(UnitContent content) {
        Map<String, InputStream> files = new HashMap<>();
        content.iterator().forEachRemaining(it -> {
            files.put(it.getKey(), new ByteArrayInputStream(it.getValue()));
        });
        return new DeploymentUnit(files);
    }
}
