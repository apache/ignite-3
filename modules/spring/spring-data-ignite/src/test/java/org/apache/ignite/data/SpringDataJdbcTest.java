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

package org.apache.ignite.data;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.springframework.data.domain.Sort.Direction.ASC;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.data.repository.Person;
import org.apache.ignite.data.repository.Person.Direction;
import org.apache.ignite.data.repository.PersonRepository;
import org.apache.ignite.data.repository.Root;
import org.apache.ignite.data.repository.RootRepository;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.support.WindowIterator;
import org.springframework.data.util.Streamable;

/**
 * This is a subset of Spring Data JDBC tests adapted from
 * <a href="https://github.com/spring-projects/spring-data-relational/tree/main/spring-data-jdbc/src/test/java/org/springframework/data/jdbc/repository">
 *     Spring Data JDBC repo</a>.
 */
@SpringBootTest(classes = TestApplication.class)
@ExtendWith(WorkDirectoryExtension.class)
@DisplayNameGeneration(SpringDataVersionDisplayNameGenerator.class)
public class SpringDataJdbcTest extends BaseIgniteAbstractTest {

    @WorkDirectory
    private static Path workDir;
    private static Cluster cluster;

    @Autowired
    PersonRepository repository;

    @Autowired
    RootRepository rootRepository;

    @BeforeAll
    static void setUp(TestInfo testInfo) {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.builder(testInfo, workDir).build();

        cluster = new Cluster(clusterConfiguration);
        cluster.startAndInit(1);

        cluster.aliveNode().sql().execute("CREATE TABLE IF NOT EXISTS Person ("
                + "    id INT,"
                + "    name VARCHAR,"
                + "    flag BOOLEAN,"
                + "    ref  BIGINT,"
                + "    direction VARCHAR,"
                + "    Primary key(id)"
                + ");");

        cluster.aliveNode().sql().execute("CREATE TABLE IF NOT EXISTS ROOT ("
                + "    ID   BIGINT PRIMARY KEY,"
                + "    NAME VARCHAR(100)"
                + ");");
        cluster.aliveNode().sql().execute("CREATE TABLE IF NOT EXISTS INTERMEDIATE ("
                + "    ID       BIGINT  PRIMARY KEY,"
                + "    NAME     VARCHAR(100),"
                + "    ROOT     BIGINT,"
                + "    ROOT_ID  BIGINT,"
                + "    ROOT_KEY INTEGER"
                + ");");
        cluster.aliveNode().sql().execute("CREATE TABLE IF NOT EXISTS LEAF ("
                + "    ID               BIGINT PRIMARY KEY,"
                + "    NAME             VARCHAR(100),"
                + "    INTERMEDIATE     BIGINT,"
                + "    INTERMEDIATE_ID  BIGINT,"
                + "    INTERMEDIATE_KEY INTEGER"
                + ");");
    }

    @BeforeEach
    void setupEach() {
        repository.deleteAll();
        rootRepository.deleteAll();
    }

    @Test
    public void savesAnEntity() {
        repository.save(Person.create());

        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    public void saveAndLoadAnEntity() {
        Person p1 = repository.save(Person.create());

        assertThat(repository.findById(p1.getId())).hasValueSatisfying(it -> {

            assertThat(it.getId()).isEqualTo(p1.getId());
            assertThat(it.getName()).isEqualTo(p1.getName());
        });
    }

    @Test
    public void insertsManyEntities() {
        Person p1 = Person.create();
        Person p2 = Person.create();

        repository.saveAll(asList(p1, p2));

        assertThat(repository.findAll())
                .extracting(Person::getId)
                .containsExactlyInAnyOrder(p1.getId(), p2.getId());
    }

    @Test
    public void existsReturnsTrueIffEntityExists() {
        Person p1 = repository.save(Person.create());

        assertThat(repository.existsById(p1.getId())).isTrue();
        assertThat(repository.existsById(p1.getId() + 1)).isFalse();
    }

    @Test
    public void findAllFindsAllEntities() {
        Person p1 = repository.save(Person.create());
        Person p2 = repository.save(Person.create());

        Iterable<Person> all = repository.findAll();

        assertThat(all)
                .extracting(Person::getId)
                .containsExactlyInAnyOrder(p1.getId(), p2.getId());
    }

    @Test
    public void findAllFindsAllSpecifiedEntities() {
        Person p1 = repository.save(Person.create());
        Person p2 = repository.save(Person.create());

        assertThat(repository.findAllById(asList(p1.getId(), p2.getId())))
                .extracting(Person::getId)
                .containsExactlyInAnyOrder(p1.getId(), p2.getId());
    }

    @Test
    public void countsEntities() {
        repository.save(Person.create());
        repository.save(Person.create());
        repository.save(Person.create());

        assertThat(repository.count()).isEqualTo(3L);
    }

    @Test
    public void deleteById() {
        Person p1 = repository.save(Person.create());
        Person p2 = repository.save(Person.create());
        Person p3 = repository.save(Person.create());

        repository.deleteById(p2.getId());

        assertThat(repository.findAll())
                .extracting(Person::getId)
                .containsExactlyInAnyOrder(p1.getId(), p3.getId());
    }

    @Test
    public void deleteByEntity() {
        Person p1 = repository.save(Person.create());
        Person p2 = repository.save(Person.create());
        Person p3 = repository.save(Person.create());

        repository.delete(p1);

        assertThat(repository.findAll())
                .extracting(Person::getId)
                .containsExactlyInAnyOrder(p2.getId(), p3.getId());
    }

    @Test
    public void deleteByList() {
        Person p1 = repository.save(Person.create());
        Person p2 = repository.save(Person.create());
        Person p3 = repository.save(Person.create());

        repository.deleteAll(asList(p1, p3));

        assertThat(repository.findAll())
                .extracting(Person::getId)
                .containsExactlyInAnyOrder(p2.getId());
    }

    @Test
    public void deleteByIdList() {
        Person p1 = repository.save(Person.create());
        Person p2 = repository.save(Person.create());
        Person p3 = repository.save(Person.create());

        repository.deleteAllById(asList(p1.getId(), p3.getId()));

        assertThat(repository.findAll())
                .extracting(Person::getId)
                .containsExactlyInAnyOrder(p2.getId());
    }

    @Test
    public void deleteAll() {
        repository.save(Person.create());
        repository.save(Person.create());
        repository.save(Person.create());

        assertThat(repository.findAll()).isNotEmpty();

        repository.deleteAll();

        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    public void update() {
        Person p1 = repository.save(Person.create());

        p1.setName("something else");
        p1.setNew(false);
        Person saved = repository.save(p1);

        assertThat(repository.findById(p1.getId())).hasValueSatisfying(it -> {
            assertThat(it.getName()).isEqualTo(saved.getName());
        });
    }

    @Test
    public void updateMany() {
        Person p1 = repository.save(Person.create());
        Person p2 = repository.save(Person.create());

        p1.setName("something else");
        p1.setNew(false);
        p2.setName("others Name");
        p2.setNew(false);

        repository.saveAll(asList(p1, p2));

        assertThat(repository.findAll())
                .extracting(Person::getName)
                .containsExactlyInAnyOrder(p1.getName(), p2.getName());
    }

    @Test
    void insertsOrUpdatesManyEntities() {
        Person p1 = repository.save(Person.create());
        p1.setName("something else");
        p1.setNew(false);
        Person p2 = Person.create();
        p2.setName("others name");
        repository.saveAll(asList(p2, p1));

        assertThat(repository.findAll())
                .extracting(Person::getName)
                .containsExactlyInAnyOrder(p1.getName(), p2.getName());
    }

    @Test
    public void findByIdReturnsEmptyWhenNoneFound() {
        // NOT saving anything, so DB is empty

        assertThat(repository.findById(-1L)).isEmpty();
    }

    @Test
    public void existsWorksAsExpected() {
        Person p1 = repository.save(Person.create());

        assertSoftly(softly -> {

            softly.assertThat(repository.existsByName(p1.getName()))
                    .describedAs("Positive")
                    .isTrue();
            softly.assertThat(repository.existsByName("not an existing name"))
                    .describedAs("Positive")
                    .isFalse();
        });
    }

    @Test
    public void existsInWorksAsExpected() {
        Person p1 = repository.save(Person.create());

        assertSoftly(softly -> {

            softly.assertThat(repository.existsByNameIn(p1.getName()))
                    .describedAs("Positive")
                    .isTrue();
            softly.assertThat(repository.existsByNameIn())
                    .describedAs("Negative")
                    .isFalse();
        });
    }

    @Test
    public void existsNotInWorksAsExpected() {
        Person dummy = repository.save(Person.create());

        assertSoftly(softly -> {

            softly.assertThat(repository.existsByNameNotIn(dummy.getName()))
                    .describedAs("Positive")
                    .isFalse();
            softly.assertThat(repository.existsByNameNotIn())
                    .describedAs("Negative")
                    .isTrue();
        });
    }

    @Test
    public void countByQueryDerivation() {
        Person p1 = Person.create();
        Person p2 = Person.create();
        p2.setName("other");
        Person three = Person.create();

        repository.saveAll(asList(p1, p2, three));

        assertThat(repository.countByName(p1.getName())).isEqualTo(2);
    }

    @Test
    public void pageByNameShouldReturnCorrectResult() {
        repository.saveAll(asList(Person.create("a1"), Person.create("a2"), Person.create("a3")));

        Page<Person> page = repository.findPageByNameContains("a", PageRequest.of(0, 5));

        assertThat(page.getContent()).hasSize(3);
        assertThat(page.getTotalElements()).isEqualTo(3);
        assertThat(page.getTotalPages()).isEqualTo(1);

        assertThat(repository.findPageByNameContains("a", PageRequest.of(0, 2)).getContent()).hasSize(2);
        assertThat(repository.findPageByNameContains("a", PageRequest.of(1, 2)).getContent()).hasSize(1);
    }

    @Test
    public void selectWithLimitShouldReturnCorrectResult() {
        repository.saveAll(asList(Person.create("a1"), Person.create("a2"), Person.create("a3")));

        List<Person> page = repository.findByNameContains("a", Limit.of(3));
        assertThat(page).hasSize(3);

        assertThat(repository.findByNameContains("a", Limit.of(2))).hasSize(2);
        assertThat(repository.findByNameContains("a", Limit.unlimited())).hasSize(3);
    }

    @Test
    public void sliceByNameShouldReturnCorrectResult() {
        repository.saveAll(asList(Person.create("a1"), Person.create("a2"), Person.create("a3")));

        Slice<Person> slice = repository.findSliceByNameContains("a", PageRequest.of(0, 5));

        assertThat(slice.getContent()).hasSize(3);
        assertThat(slice.hasNext()).isFalse();

        slice = repository.findSliceByNameContains("a", PageRequest.of(0, 2));

        assertThat(slice.getContent()).hasSize(2);
        assertThat(slice.hasNext()).isTrue();
    }

    @Test
    void derivedQueryWithBooleanLiteralFindsCorrectValues() {
        repository.save(Person.create());
        Person p1 = Person.create();
        p1.setFlag(true);
        p1 = repository.save(p1);

        List<Person> result = repository.findByFlagTrue();

        assertThat(result).extracting(Person::getId).containsExactly(p1.getId());
    }

    @Test
    void queryBySimpleReference() {
        Person p1 = repository.save(Person.create());
        Person p2 = Person.create();
        p2.setRef(AggregateReference.to(p1.getId()));
        p2 = repository.save(p2);

        List<Person> result = repository.findByRef(p1.getId().intValue());

        assertThat(result).extracting(Person::getId).containsExactly(p2.getId());
    }

    @Test
    void queryByAggregateReference() {
        Person p1 = repository.save(Person.create());
        Person p2 = Person.create();
        p2.setRef(AggregateReference.to(p1.getId()));
        p2 = repository.save(p2);

        List<Person> result = repository.findByRef(p2.getRef());

        assertThat(result).extracting(Person::getId).containsExactly(p2.getId());
    }

    @Test
    void queryByEnumTypeIn() {
        Person p1 = Person.create("p1");
        p1.setDirection(Direction.LEFT);
        Person p2 = Person.create("p2");
        p2.setDirection(Direction.CENTER);
        Person p3 = Person.create("p3");
        p3.setDirection(Direction.RIGHT);
        repository.saveAll(asList(p1, p2, p3));

        assertThat(repository.findByEnumTypeIn(Set.of(Direction.LEFT, Direction.RIGHT)))
                .extracting(Person::getDirection).containsExactlyInAnyOrder(Direction.LEFT, Direction.RIGHT);
    }

    @Test
    void queryByEnumTypeEqual() {
        Person p1 = Person.create("p1");
        p1.setDirection(Direction.LEFT);
        Person p2 = Person.create("p2");
        p2.setDirection(Direction.CENTER);
        Person p3 = Person.create("p3");
        p3.setDirection(Direction.RIGHT);
        repository.saveAll(asList(p1, p2, p3));

        assertThat(repository.findByEnumType(Direction.CENTER)).extracting(Person::getDirection)
                .containsExactlyInAnyOrder(Direction.CENTER);
    }

    @Test
    void manyInsertsWithNestedEntities() {
        Root root1 = Root.create("root1");
        Root root2 = Root.create("root2");

        List<Root> savedRoots = rootRepository.saveAll(asList(root1, root2));

        List<Root> reloadedRoots = rootRepository.findAllByOrderByIdAsc();
        assertThat(reloadedRoots).isEqualTo(savedRoots);
        assertThat(reloadedRoots).hasSize(2);
    }

    @Test
    void findOneByExampleShouldGetOne() {
        Person p1 = Person.create();
        p1.setFlag(true);
        repository.save(p1);

        Person p2 = Person.create();
        p2.setName("Diego");
        repository.save(p2);

        Example<Person> diegoExample = Example.of(p2);
        Optional<Person> foundExampleDiego = repository.findOne(diegoExample);

        assertThat(foundExampleDiego.get().getName()).isEqualTo("Diego");
    }

    @Test
    void findOneByExampleMultipleMatchShouldGetOne() {
        repository.save(Person.create());
        repository.save(Person.create());

        Example<Person> example = Example.of(new Person());

        assertThatThrownBy(() -> repository.findOne(example)).isInstanceOf(IncorrectResultSizeDataAccessException.class)
                .hasMessageContaining("expected 1, actual 2");
    }

    @Test
    void findOneByExampleShouldGetNone() {
        Person p1 = Person.create();
        p1.setFlag(true);
        repository.save(p1);

        Example<Person> diegoExample = Example.of(Person.create("NotExisting"));

        Optional<Person> foundExampleDiego = repository.findOne(diegoExample);

        assertThat(foundExampleDiego).isNotPresent();
    }

    @Test
    void findAllByExampleShouldGetOne() {
        Person p1 = Person.create();
        p1.setFlag(true);
        repository.save(p1);

        Person p2 = Person.create();
        p2.setName("Diego");
        repository.save(p2);

        Example<Person> example = Example.of(Person.create(null, "Diego"));

        Iterable<Person> allFound = repository.findAll(example);

        assertThat(allFound).extracting(Person::getName)
                .containsExactly(example.getProbe().getName());
    }

    @Test
    void findAllByExampleMultipleMatchShouldGetOne() {
        repository.save(Person.create());
        repository.save(Person.create());

        Example<Person> example = Example.of(new Person(null, "Name"));

        Iterable<Person> allFound = repository.findAll(example);

        assertThat(allFound)
                .hasSize(2)
                .extracting(Person::getName)
                .containsOnly(example.getProbe().getName());
    }

    @Test
    void findAllByExampleShouldGetNone() {
        Person p1 = Person.create();
        p1.setFlag(true);

        repository.save(p1);

        Example<Person> example = Example.of(Person.create("NotExisting"));

        Iterable<Person> allFound = repository.findAll(example);

        assertThat(allFound).isEmpty();
    }

    @Test
    void findAllByExamplePageableShouldGetOne() {
        Person p1 = Person.create();
        p1.setFlag(true);

        repository.save(p1);

        Person p2 = Person.create();
        p2.setName("Diego");

        repository.save(p2);

        Example<Person> example = Example.of(p2);
        Pageable pageRequest = PageRequest.of(0, 10);

        Iterable<Person> allFound = repository.findAll(example, pageRequest);

        assertThat(allFound).extracting(Person::getName)
                .containsExactly(example.getProbe().getName());
    }

    @Test
    void findAllByExamplePageableMultipleMatchShouldGetOne() {
        repository.save(Person.create());
        repository.save(Person.create());

        Example<Person> example = Example.of(new Person(null, "Name"));
        Pageable pageRequest = PageRequest.of(0, 10);

        Iterable<Person> allFound = repository.findAll(example, pageRequest);

        assertThat(allFound)
                .hasSize(2)
                .extracting(Person::getName)
                .containsOnly(example.getProbe().getName());
    }

    @Test
    void findAllByExamplePageableShouldGetNone() {
        Person p1 = Person.create();
        p1.setFlag(true);

        repository.save(p1);

        Example<Person> example = Example.of(Person.create("NotExisting"));
        Pageable pageRequest = PageRequest.of(0, 10);

        Iterable<Person> allFound = repository.findAll(example, pageRequest);

        assertThat(allFound).isEmpty();
    }

    @Test
    void findAllByExamplePageableOutsidePageShouldGetNone() {
        repository.save(Person.create());
        repository.save(Person.create());

        Example<Person> example = Example.of(Person.create());
        Pageable pageRequest = PageRequest.of(10, 10);

        Iterable<Person> allFound = repository.findAll(example, pageRequest);

        assertThat(allFound)
                .isNotNull()
                .isEmpty();
    }

    @ParameterizedTest
    @MethodSource("findAllByExamplePageableSource")
    void findAllByExamplePageable(Pageable pageRequest, int size, int totalPages, List<String> notContains) {
        for (int i = 0; i < 100; i++) {
            Person p = Person.create();
            p.setFlag(true);
            p.setName("" + i);

            repository.save(p);
        }

        Person p = Person.create();
        p.setId(null);
        p.setName(null);
        p.setFlag(true);

        Example<Person> example = Example.of(p);

        Page<Person> allFound = repository.findAll(example, pageRequest);

        // page has correct size
        assertThat(allFound)
                .isNotNull()
                .hasSize(size);

        // correct number of total
        assertThat(allFound.getTotalElements()).isEqualTo(100);

        assertThat(allFound.getTotalPages()).isEqualTo(totalPages);

        if (!notContains.isEmpty()) {
            assertThat(allFound)
                    .extracting(Person::getName)
                    .doesNotContain(notContains.toArray(new String[0]));
        }
    }

    static Stream<Arguments> findAllByExamplePageableSource() {
        return Stream.of(
                Arguments.of(PageRequest.of(0, 3, ASC, "id"), 3, 34, asList("3", "4", "100")),
                Arguments.of(PageRequest.of(1, 10, ASC, "id"), 10, 10, asList("9", "20", "30")),
                Arguments.of(PageRequest.of(2, 10, ASC, "id"), 10, 10, asList("1", "2", "3")),
                Arguments.of(PageRequest.of(33, 3, ASC, "id"), 1, 34, Collections.emptyList()),
                Arguments.of(PageRequest.of(36, 3, ASC, "id"), 0, 34, Collections.emptyList()),
                Arguments.of(PageRequest.of(0, 10000, ASC, "id"), 100, 1, Collections.emptyList()),
                Arguments.of(PageRequest.of(100, 10000, ASC, "id"), 0, 1, Collections.emptyList())
        );
    }

    @Test
    void existsByExampleShouldGetOne() {
        Person p1 = Person.create();
        p1.setFlag(true);
        repository.save(p1);

        Person p2 = Person.create();
        p2.setName("Diego");
        repository.save(p2);

        Example<Person> example = Example.of(Person.create(null, "Diego"));

        boolean exists = repository.exists(example);

        assertThat(exists).isTrue();
    }

    @Test
    void existsByExampleMultipleMatchShouldGetOne() {
        Person p1 = Person.create();
        repository.save(p1);

        Person p2 = Person.create();
        repository.save(p2);

        Example<Person> example = Example.of(new Person());

        boolean exists = repository.exists(example);
        assertThat(exists).isTrue();
    }

    @Test
    void existsByExampleShouldGetNone() {
        Person p1 = Person.create();
        p1.setFlag(true);

        repository.save(p1);

        Example<Person> example = Example.of(Person.create("NotExisting"));

        boolean exists = repository.exists(example);

        assertThat(exists).isFalse();
    }

    @Test
    void countByExampleShouldGetOne() {
        Person p1 = Person.create();
        p1.setFlag(true);

        repository.save(p1);

        Person p2 = Person.create();
        p2.setName("Diego");

        repository.save(p2);

        Example<Person> example = Example.of(p2);

        long count = repository.count(example);

        assertThat(count).isOne();
    }

    @Test
    void countByExampleMultipleMatchShouldGetOne() {
        Person p1 = Person.create();
        repository.save(p1);

        Person p2 = Person.create();
        repository.save(p2);

        Example<Person> example = Example.of(new Person());

        long count = repository.count(example);
        assertThat(count).isEqualTo(2);
    }

    @Test
    void countByExampleShouldGetNone() {
        Person p1 = Person.create();
        p1.setFlag(true);

        repository.save(p1);

        Example<Person> example = Example.of(Person.create("NotExisting"));

        long count = repository.count(example);

        assertThat(count).isNotNull().isZero();
    }

    @Test
    void findByScrollPosition() {
        Person p1 = Person.create("p1");
        p1.setFlag(true);

        Person p2 = Person.create("p2");
        p2.setFlag(true);

        Person p3 = Person.create("p3");
        p3.setFlag(true);

        Person p4 = Person.create("p4");
        p4.setFlag(false);

        repository.saveAll(asList(p1, p2, p3, p4));

        Example<Person> example = Example.of(p1, ExampleMatcher.matching().withIgnorePaths("name", "id"));

        Window<Person> first = repository.findBy(example, q -> q.limit(2).sortBy(Sort.by("name")))
                .scroll(ScrollPosition.offset());
        assertThat(first.map(Person::getName)).containsExactly("p1", "p2");

        Window<Person> second = repository.findBy(example, q -> q.limit(2).sortBy(Sort.by("name")))
                .scroll(ScrollPosition.offset(1));
        assertThat(second.map(Person::getName)).containsExactly("p3");

        WindowIterator<Person> iterator = WindowIterator.of(
                        scrollPosition -> repository.findBy(example, q -> q.limit(2).sortBy(Sort.by("name")).scroll(scrollPosition)))
                .startingAt(ScrollPosition.offset());

        List<String> result = Streamable.of(() -> iterator).stream().map(Person::getName).toList();

        assertThat(result).hasSize(3).containsExactly("p1", "p2", "p3");
    }
}
