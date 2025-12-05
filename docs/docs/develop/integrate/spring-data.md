---
id: spring-data
title: Spring Data Integration
---

Apache Ignite 3 provides a Spring Data JDBC dialect that enables repository-based data access. The dialect registers automatically via Spring's SPI mechanism, allowing standard Spring Data repositories to work with Ignite tables.

## Prerequisites

- Java 17 or later
- Spring Boot 3.x with Spring Data JDBC
- Running Ignite 3 cluster
- Tables created in Ignite before repository operations

## Installation

Add the Spring Data Ignite dependency along with the Ignite JDBC driver.

**Maven:**

```xml
<dependency>
    <groupId>org.apache.ignite</groupId>
    <artifactId>spring-data-ignite</artifactId>
    <version>3.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.ignite</groupId>
    <artifactId>ignite-jdbc</artifactId>
    <version>3.0.0</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'org.apache.ignite:spring-data-ignite:3.0.0'
implementation 'org.apache.ignite:ignite-jdbc:3.0.0'
```

## Configuration

Configure the JDBC datasource in `application.properties`:

```properties
spring.datasource.url=jdbc:ignite:thin://localhost:10800
spring.datasource.driver-class-name=org.apache.ignite.jdbc.IgniteJdbcDriver
```

For multiple nodes:

```properties
spring.datasource.url=jdbc:ignite:thin://node1:10800,node2:10800,node3:10800
```

## Application Setup

Enable JDBC repositories in your Spring Boot application:

```java
@EnableJdbcRepositories
@SpringBootApplication
public class MyApplication {

    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }
}
```

## Defining Entities

Entities map to Ignite tables. Use Spring Data annotations to define the mapping:

```java
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("PERSON")
public class Person {

    @Id
    private Long id;
    private String name;
    private String email;

    public Person() {}

    public Person(Long id, String name, String email) {
        this.id = id;
        this.name = name;
        this.email = email;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}
```

Create the corresponding table in Ignite before using the repository:

```sql
CREATE TABLE PERSON (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    email VARCHAR
);
```

## Repository Definition

Define a repository interface extending `CrudRepository`:

```java
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PersonRepository extends CrudRepository<Person, Long> {
}
```

## CRUD Operations

The `CrudRepository` interface provides standard data access methods:

```java
@Service
public class PersonService {

    private final PersonRepository repository;

    public PersonService(PersonRepository repository) {
        this.repository = repository;
    }

    public Person save(Person person) {
        return repository.save(person);
    }

    public Optional<Person> findById(Long id) {
        return repository.findById(id);
    }

    public Iterable<Person> findAll() {
        return repository.findAll();
    }

    public void deleteById(Long id) {
        repository.deleteById(id);
    }

    public long count() {
        return repository.count();
    }

    public boolean existsById(Long id) {
        return repository.existsById(id);
    }
}
```

## Derived Query Methods

Spring Data generates queries from method names:

```java
@Repository
public interface PersonRepository extends CrudRepository<Person, Long> {

    // SELECT * FROM PERSON WHERE name = ?
    List<Person> findByName(String name);

    // SELECT * FROM PERSON WHERE name LIKE '%value%'
    List<Person> findByNameContains(String namePart);

    // SELECT * FROM PERSON WHERE email = ?
    Optional<Person> findByEmail(String email);

    // SELECT COUNT(*) FROM PERSON WHERE name = ?
    int countByName(String name);

    // SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END FROM PERSON WHERE name = ?
    boolean existsByName(String name);

    // SELECT * FROM PERSON WHERE name IN (?, ?, ...)
    List<Person> findByNameIn(Collection<String> names);
}
```

## Custom Queries

Use `@Query` for explicit SQL:

```java
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.query.Param;

@Repository
public interface PersonRepository extends CrudRepository<Person, Long> {

    @Query("SELECT * FROM PERSON WHERE name = :name AND email = :email")
    Optional<Person> findByNameAndEmail(@Param("name") String name, @Param("email") String email);

    @Query("SELECT * FROM PERSON WHERE name IN (:names)")
    List<Person> findByNames(@Param("names") Set<String> names);
}
```

## Pagination

Spring Data supports paginated queries:

```java
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

@Repository
public interface PersonRepository extends CrudRepository<Person, Long> {

    // Returns Page with total count
    Page<Person> findByNameContains(String namePart, Pageable pageable);

    // Returns Slice without total count (more efficient for large datasets)
    Slice<Person> findSliceByNameContains(String namePart, Pageable pageable);
}
```

Usage:

```java
@Service
public class PersonService {

    private final PersonRepository repository;

    public PersonService(PersonRepository repository) {
        this.repository = repository;
    }

    public Page<Person> getPage(String namePart, int page, int size) {
        PageRequest pageRequest = PageRequest.of(page, size, Sort.by("name"));
        return repository.findByNameContains(namePart, pageRequest);
    }

    public void processAllPersons(String namePart) {
        Pageable pageable = PageRequest.of(0, 100);
        Slice<Person> slice;

        do {
            slice = repository.findSliceByNameContains(namePart, pageable);
            slice.getContent().forEach(this::process);
            pageable = slice.nextPageable();
        } while (slice.hasNext());
    }

    private void process(Person person) {
        // Process person
    }
}
```

## Query by Example

For dynamic queries based on entity instances, extend `QueryByExampleExecutor`:

```java
import org.springframework.data.repository.query.QueryByExampleExecutor;

@Repository
public interface PersonRepository extends CrudRepository<Person, Long>, QueryByExampleExecutor<Person> {
}
```

Usage:

```java
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;

@Service
public class PersonService {

    private final PersonRepository repository;

    public PersonService(PersonRepository repository) {
        this.repository = repository;
    }

    public List<Person> findByExample(String name, String email) {
        Person probe = new Person();
        probe.setName(name);
        probe.setEmail(email);

        // Match non-null properties
        Example<Person> example = Example.of(probe);
        return (List<Person>) repository.findAll(example);
    }

    public List<Person> findByNameStartsWith(String prefix) {
        Person probe = new Person();
        probe.setName(prefix);

        ExampleMatcher matcher = ExampleMatcher.matching()
            .withMatcher("name", ExampleMatcher.GenericPropertyMatchers.startsWith())
            .withIgnorePaths("id", "email");

        Example<Person> example = Example.of(probe, matcher);
        return (List<Person>) repository.findAll(example);
    }
}
```

## Handling Entity State

Ignite tables do not auto-generate IDs. Implement `Persistable` to control insert vs update behavior:

```java
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;

public class Person implements Persistable<Long> {

    @Id
    private Long id;
    private String name;

    @Transient
    private boolean isNew = true;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public void setNew(boolean newValue) {
        this.isNew = newValue;
    }

    // After loading from database, mark as not new
    public void markNotNew() {
        this.isNew = false;
    }

    // Other getters and setters
}
```

When updating an existing entity:

```java
Person person = repository.findById(1L).orElseThrow();
person.setName("Updated Name");
person.setNew(false);  // Prevents INSERT, performs UPDATE
repository.save(person);
```

## Supported Features

The Ignite dialect supports:

| Feature | Status |
|---------|--------|
| CrudRepository | Supported |
| PagingAndSortingRepository | Supported |
| QueryByExampleExecutor | Supported |
| Derived query methods | Supported |
| @Query annotations | Supported |
| Page and Slice | Supported |
| Sort | Supported |
| Limit | Supported |
| Enum types | Supported |
| Array columns | Supported |

## Limitations

- Ignite does not auto-generate primary keys. Provide ID values explicitly.
- Locking clauses (`@Lock`) are not supported. The dialect returns empty lock clauses.
- Single query loading for related entities is not supported. Related entities require separate queries.

## Next Steps

- [Spring Boot Integration](./spring-boot) - Auto-configured IgniteClient
- [JDBC Driver](../connect-to-ignite/jdbc) - JDBC connection details
- [SQL Reference](../../sql/) - SQL syntax for table creation
