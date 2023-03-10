package com.qxn.sqoophivebyjava;
public class Person  {
    private int id;
    private int a;

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", a=" + a +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public Person(int id, int a) {
        this.id = id;
        this.a = a;
    }
}
