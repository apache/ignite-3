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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that set specified zone as default.
 */
public class AlterZoneSetDefaultCatalogCommand extends AbstractZoneCommand {
    /** Returns builder to create a command that set specified zone as default. */
    public static Builder builder() {
        return new AlterZoneSetDefaultCatalogCommand.Builder();
    }

    /**
     * Constructor.
     *
     * @param zoneName Name of the zone.
     * @throws CatalogValidationException if any of restrictions above is violated.
     */
    private AlterZoneSetDefaultCatalogCommand(String zoneName) throws CatalogValidationException {
        super(zoneName);
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        // TODO https://issues.apache.org/jira/browse/IGNITE-19687
        if (zoneName.equals(DEFAULT_ZONE_NAME)) {
            return Collections.emptyList();
        }

        throw new UnsupportedOperationException();
    }

    /**
     * Builder of a command that set specified zone as default.
     */
    public static class Builder implements AbstractZoneCommandBuilder<Builder> {
        private String zoneName;

        @Override
        public Builder zoneName(String zoneName) {
            this.zoneName = zoneName;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new AlterZoneSetDefaultCatalogCommand(zoneName);
        }
    }
}
