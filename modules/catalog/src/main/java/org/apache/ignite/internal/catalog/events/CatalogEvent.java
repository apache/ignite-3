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

package org.apache.ignite.internal.catalog.events;

import org.apache.ignite.internal.event.Event;

/**
 * Catalog management events.
 */
public enum CatalogEvent implements Event {
    /** This event is fired, when a table was created in Catalog. */
    TABLE_CREATE,

    /** This event is fired, when a table was dropped in Catalog. */
    TABLE_DROP,

    /** This event is fired when a table has been renamed or a column has been modified, added to, or removed from a table. */
    TABLE_ALTER,

    /** This event is fired, when an index was created in Catalog. */
    INDEX_CREATE,

    /** This event is fired, when an index was dropped in Catalog. */
    INDEX_DROP,

    /** This event is fired when the index becomes available, i.e. the index has been built. */
    INDEX_AVAILABLE,

    /** This event is fired, when a distribution zone was created in Catalog. */
    ZONE_CREATE,

    /** This event is fired, when a distribution zone was dropped in Catalog. */
    ZONE_DROP,

    /** This event is fired, when a distribution zone was changed in Catalog. */
    ZONE_ALTER,

    /** This event is fired, when a system view was created in Catalog. */
    SYSTEM_VIEW_CREATE,
}
