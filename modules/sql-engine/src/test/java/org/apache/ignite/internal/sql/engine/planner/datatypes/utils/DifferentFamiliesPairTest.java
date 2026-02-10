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

package org.apache.ignite.internal.sql.engine.planner.datatypes.utils;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

/**
 * This test makes sure that all possible type pairs from different families are presented in {@link DifferentFamiliesPair} enum.
 */
class DifferentFamiliesPairTest {
    @Test
    void makeSureAllTypePairsFromDifferentTypeFamiliesAreCovered() {
        ColumnType[] allSpecs = NativeType.nativeTypes();
        Set<IgniteBiTuple<ColumnType, ColumnType>> allPairs = new HashSet<>();
        for (int i = 0; i < allSpecs.length - 1; i++) {
            ColumnType firstType = allSpecs[i];
            RelDataType firstRelType = relTypeFromTypeSpec(firstType);

            for (int j = i + 1; j < allSpecs.length; j++) {
                ColumnType secondType = allSpecs[j];
                RelDataType secondRelType = relTypeFromTypeSpec(secondType);

                if (firstRelType.getFamily().equals(secondRelType.getFamily())) {
                    // we need to check only different families
                    continue;
                }

                allPairs.add(new IgniteBiTuple<>(firstType, secondType));
            }
        }

        List<IgniteBiTuple<ColumnType, ColumnType>> actualPairs = Stream.of(DifferentFamiliesPair.values())
                .flatMap(pair -> {
                    ColumnType firstSpec = pair.first().spec();
                    ColumnType secondSpec = pair.second().spec();

                    return Stream.of(
                            new IgniteBiTuple<>(firstSpec, secondSpec), new IgniteBiTuple<>(secondSpec, firstSpec)
                    );
                })
                .collect(Collectors.toList());

        actualPairs.forEach(allPairs::remove);

        assertThat(allPairs, empty());
    }

    private static RelDataType relTypeFromTypeSpec(ColumnType spec) {
        // Values of precision, scale, and length doesn't matter, because we only interested in type specs and families,
        // and types created from the same spec but different precision/scale/length still belongs to the same family.
        NativeType type = TypeUtils.columnType2NativeType(spec, 3, 0, 10);

        return native2relationalType(Commons.typeFactory(), type);
    }
}
