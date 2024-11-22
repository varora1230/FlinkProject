package org.data.pojo;

/**
 * Person represents an individual with basic details such as name, address,
 * date of birth (dob), and age.
 */
public class Person {

    private String name;

    private String address;

    private String dob;

    private int age;

    /**
     * Default constructor.
     */
    public Person() {}

    /**
     * @param name    The name of the person.
     * @param address The address of the person.
     * @param dob     The date of birth (DOB) of the person.
     * @param age     The age of the person.
     */
    public Person(String name, String address, String dob, int age) {
        this.name = name;
        this.address = address;
        this.dob = dob;
        this.age = age;
    }

    /**
     * @return The person's name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name The name to set.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return The person's address.
     */
    public String getAddress() {
        return address;
    }

    /**
     * @param address The address to set.
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * @return The person's DOB as a string.
     */
    public String getDob() {
        return dob;
    }

    /**
     * @param dob The DOB to set, expected to follow a specific format (e.g., "dd-MM-yyyy").
     */
    public void setDob(String dob) {
        this.dob = dob;
    }

    /**
     * @return The person's age.
     */
    public int getAge() {
        return age;
    }

    /**
     * @param age The age to set.
     */
    public void setAge(int age) {
        this.age = age;
    }
}
