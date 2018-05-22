package it.polito.gjcode.spark.ex50.dataset;

import java.io.Serializable;

@SuppressWarnings("serial")
public class NameSurname implements Serializable {
	private String name_surname;

	public String getName_surname() {
		return name_surname;
	}

	public void setName_surname(String name_surname) {
		this.name_surname = name_surname;
	}

}
