package org.apache.ignite.example.table;

/**
 * Represents a Person entity with database mapping.
 *
 * Database schema:
 * - id: int PRIMARY KEY
 * - city_id: int
 * - name: varchar
 * - age: int
 * - company: varchar
 */
public class Person {
    private int id;
    private int cityId;
    private String name;
    private int age;
    private String company;

    public Person() {}

    public Person(int id, int cityId, String name, int age, String company) {
        this.id = id;
        this.cityId = cityId;
        this.name = name;
        this.age = age;
        this.company = company;
    }

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    public int getCityId() { return cityId; }
    public void setCityId(int cityId) { this.cityId = cityId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }

    public String getCompany() { return company; }
    public void setCompany(String company) { this.company = company; }
}
