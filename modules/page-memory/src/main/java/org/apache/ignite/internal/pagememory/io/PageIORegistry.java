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

package org.apache.ignite.internal.pagememory.io;

import java.nio.ByteBuffer;
import java.util.ServiceLoader;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Page IO Registry. This component registers and provides all available {@link PageIO} types.
 */
public class PageIORegistry {
    /** Arrays of {@link IOVersions} for fast access. Element 0 is reserved. */
    private IOVersions<?>[] ioVersions = new IOVersions[65535];

    public void loadFromServiceLoader() {
        ServiceLoader<PageIOModule> serviceLoader = ServiceLoader.load(PageIOModule.class);

        for (PageIOModule pageIOModule : serviceLoader) {
            for (IOVersions<?> ios : pageIOModule.ioVersions()) {
                assert ios.getType() != 0 : "Type 0 is reserved and can't be used: " + ios;
                assert ioVersions[ios.getType()] == null || ioVersions[ios.getType()].equals(ios) : "Duplication IOVersions found: " + ios;

                ioVersions[ios.getType()] = ios;
            }
        }
    }

    /**
     * Returns resolved {@link PageIO} by the {@link ByteBuffer} that contains the page.
     *
     * @param pageBuf Byte buffer with page content.
     * @return Resolved page IO instance.
     * @throws IgniteInternalCheckedException If page type or version are invalid or not registered.
     */
    public PageIO resolve(ByteBuffer pageBuf) throws IgniteInternalCheckedException {
        return resolve(PageIO.getType(pageBuf), PageIO.getVersion(pageBuf));
    }


    /**
     * Returns resolved {@link PageIO} by the page address.
     *
     * @param pageAddr Memory address pointing to the page content.
     * @return Resolved page IO instance.
     * @throws IgniteInternalCheckedException If page type or version are invalid or not registered.
     */
    public final PageIO resolve(long pageAddr) throws IgniteInternalCheckedException {
        return resolve(PageIO.getType(pageAddr), PageIO.getVersion(pageAddr));
    }

    /**
     * Returns resolved {@link PageIO} by the type and the version.
     *
     * @param type Page IO type.
     * @param ver  Page IO version.
     * @return Resolved page IO instance.
     * @throws IgniteInternalCheckedException If page type or version are invalid or not registered.
     */
    public PageIO resolve(int type, int ver) throws IgniteInternalCheckedException {
        if (type <= 0 || type >= 65535) {
            throw new IgniteInternalCheckedException("Unknown page IO type: " + type);
        }

        IOVersions<?> ios = ioVersions[type];

        if (ios == null) {
            throw new IgniteInternalCheckedException("Unknown page IO type: " + type);
        }

        return ios.forVersion(ver);
    }
}
