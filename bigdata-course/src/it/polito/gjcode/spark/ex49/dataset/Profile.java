package it.polito.gjcode.spark.ex49.dataset;

import scala.Serializable;

@SuppressWarnings("serial")
public class Profile implements Serializable {

	private String name;
	private String surname;
	private int age;

	public Profile(String name, String surname, int age) {
		this.name = name;
		this.surname = surname;
		this.age = age;
	}

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