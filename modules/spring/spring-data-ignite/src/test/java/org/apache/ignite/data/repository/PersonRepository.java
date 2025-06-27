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

package org.apache.ignite.data.repository;

import java.util.List;
import java.util.Set;
import org.apache.ignite.data.repository.Person.Direction;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.springframework.stereotype.Repository;

/**
 * Repository for {@link Person}.
 */
@Repository
public interface PersonRepository extends CrudRepository<Person, Long>, QueryByExampleExecutor<Person> {

    boolean existsByName(String name);

    boolean existsByNameIn(String... names);

    boolean existsByNameNotIn(String... names);

    int countByName(String name);

    Page<Person> findPageByNameContains(String name, Pageable pageable);

    List<Person> findByNameContains(String name, Limit limit);

    Slice<Person> findSliceByNameContains(String name, Pageable pageable);

    List<Person> findByFlagTrue();

    List<Person> findByRef(int ref);

    List<Person> findByRef(AggregateReference<Person, Long> ref);

    @Query("SELECT * FROM PERSON WHERE DIRECTION IN (:directions)")
    List<Person> findByEnumTypeIn(@Param("directions") Set<Direction> directions);

    @Query("SELECT * FROM PERSON WHERE DIRECTION = :direction")
    List<Person> findByEnumType(@Param("direction")Direction direction);
}
