---
id: spring-data
title: Spring Data Integration
---

Apache Ignite 3 provides a Spring Data JDBC dialect that enables repository-based data access. Combined with Spring Boot's JDBC starter, this allows standard Spring Data repositories to work with Ignite tables through familiar patterns like `CrudRepository` and derived query methods.

## Prerequisites

- Java 17 or later
- Spring Boot 3.x with Spring Data JDBC
- Running Ignite 3 cluster
- Tables created in Ignite before repository operations

## Installation

Spring Data integration requires three dependencies:

- `spring-boot-starter-data-jdbc` (from Spring) provides the Spring Data JDBC framework
- `spring-data-ignite` (from Apache Ignite) provides the SQL dialect for Ignite-compatible query generation
- `ignite-jdbc` (from Apache Ignite) provides the JDBC driver for database connectivity

The Ignite artifact versions must match your Apache Ignite cluster version.

**Maven:**

```xml
<properties>
    <ignite.version>3.1.0</ignite.version>
</properties>

<!-- Spring Data JDBC framework -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jdbc</artifactId>
</dependency>

<!-- Ignite SQL dialect for Spring Data -->
<dependency>
    <groupId>org.apache.ignite</groupId>
    <artifactId>spring-data-ignite</artifactId>
    <version>${ignite.version}</version>
</dependency>

<!-- Ignite JDBC driver -->
<dependency>
    <groupId>org.apache.ignite</groupId>
    <artifactId>ignite-jdbc</artifactId>
    <version>${ignite.version}</version>
</dependency>
```

**Gradle:**

```groovy
ext {
    igniteVersion = '3.1.0'
}

// Spring Data JDBC framework
implementation 'org.springframework.boot:spring-boot-starter-data-jdbc'

// Ignite SQL dialect for Spring Data
implementation "org.apache.ignite:spring-data-ignite:${igniteVersion}"

// Ignite JDBC driver
implementation "org.apache.ignite:ignite-jdbc:${igniteVersion}"
```

:::note Version Matching
The `spring-data-ignite` and `ignite-jdbc` artifacts are released as part of Apache Ignite, so their versions match the Ignite release version. For Ignite 3.1.0, use version `3.1.0` for both artifacts.
:::

## Configuration

### Datasource Properties

Configure the JDBC datasource in `application.properties`:

```properties
spring.datasource.url=jdbc:ignite:thin://localhost:10800
spring.datasource.driver-class-name=org.apache.ignite.jdbc.IgniteJdbcDriver
```

For multiple nodes:

```properties
spring.datasource.url=jdbc:ignite:thin://node1:10800,node2:10800,node3:10800
```

### SQL Dialect Registration

Spring Data JDBC needs to generate database-specific SQL for operations like pagination, identity columns, and certain functions. The `spring-data-ignite` artifact includes an `IgniteDialectProvider` that teaches Spring Data how to generate Ignite-compatible SQL.

The dialect provider is registered via Spring's SPI mechanism. Create the file `src/main/resources/META-INF/spring.factories` with the following content:

```properties
org.springframework.data.jdbc.repository.config.DialectResolver$JdbcDialectProvider=org.apache.ignite.data.IgniteDialectProvider
```

Without this configuration, Spring Data falls back to generic ANSI SQL, which works for basic queries but may fail for database-specific operations.

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
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("PERSON")
public class Person {

    @Id
    private Long id;
    private String name;
    private String email;

    @Column("COUNTRYCODE")
    private String countryCode;  // Maps to COUNTRYCODE column

    public Person() {}

    public Person(Long id, String name, String email, String countryCode) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.countryCode = countryCode;
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getCountryCode() { return countryCode; }
    public void setCountryCode(String countryCode) { this.countryCode = countryCode; }
}
```

Create the corresponding table in Ignite before using the repository:

```sql
CREATE TABLE PERSON (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    COUNTRYCODE VARCHAR
);
```

Key annotations:

- `@Table` maps the class to a specific table name
- `@Id` marks the primary key field
- `@Column` maps a field to a column when names differ (Java's `countryCode` to SQL's `COUNTRYCODE`). Fields without `@Column` map by convention based on field name.

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
- [SQL Reference](../../sql) - SQL syntax for table creation
