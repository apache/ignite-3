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

package org.apache.ignite.internal.rest;

import io.micronaut.context.event.BeanDestroyedEvent;
import io.micronaut.context.event.BeanDestroyedEventListener;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Cleans up resources of destroyed beans.
 */
@Singleton
public class ResourceCleaner implements BeanDestroyedEventListener<ResourceHolder> {
    private static final IgniteLogger LOG = Loggers.forClass(ResourceCleaner.class);

    @Override
    public void onDestroyed(BeanDestroyedEvent<ResourceHolder> event) {
        ResourceHolder bean = event.getBean();
        if (bean != null) {
            LOG.debug("Cleaning bean {}", bean);
            bean.cleanResources();
        }
    }
}
