package it.polito.gjcode.datasetTest;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;

public class Person implements Serializable {

	private static final long serialVersionUID = 1L;
	private String nome, cognome;
	private int eta;
	private Date dataRegistrazione;
	private Time oraRegistrazione;
	private double importoInserito;

	public Person(String nome, String cognome, int eta) {
		this.nome = nome;
		this.cognome = cognome;
		this.eta = eta;
	}

	public String getNome() {
		return nome;
	}

	public void setNome(String nome) {
		this.nome = nome;
	}

	public String getCognome() {
		return cognome;
	}

	public void setCognome(String cognome) {
		this.cognome = cognome;
	}

	public int getEta() {
		return eta;
	}

	public void setEta(int eta) {
		this.eta = eta;
	}

	public Date getDataRegistrazione() {
		return dataRegistrazione;
	}

	public void setDataRegistrazione(Date dataRegistrazione) {
		this.dataRegistrazione = dataRegistrazione;
	}

	public Time getOraRegistrazione() {
		return oraRegistrazione;
	}

	public void setOraRegistrazione(Time oraRegistrazione) {
		this.oraRegistrazione = oraRegistrazione;
	}

	public double getImportoInserito() {
		return importoInserito;
	}

	public void setImportoInserito(double importoInserito) {
		this.importoInserito = importoInserito;
	}
	
	

}
