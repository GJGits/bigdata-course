package it.polito.gjcode.spark.ex50.dataset;

import java.io.Serializable;

/* */

public class Profile implements Serializable {
	private static final long serialVersionUID = 1L;
	private String name;
	private String surname;
	private int age;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

}
