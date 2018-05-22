package it.polito.gjcode.spark.ex49.dataset;

import scala.Serializable;

@SuppressWarnings("serial")
public class ProfileNewAge implements Serializable {

	private String name;
	private String surname;
	private String age;

	public ProfileNewAge(Profile profile) {
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

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public void setAge(int age) {
		int min = (age / 10) * 10;
		int max = min + 1;
		this.age = "[" + min + "-" + max + "]";
	}

}
