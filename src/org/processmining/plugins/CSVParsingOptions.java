package org.processmining.plugins;

public class CSVParsingOptions {
	private String charSet;
	private String dateFormat;
	private String decimalSeparator;
	private String escapeSequence;
	private String lineEnding;
	private char quoteSequence;
	private String separatorSequence;
	private String thousandSeparator;
	
	public CSVParsingOptions() {
		this.quoteSequence = '"';
        this.escapeSequence = "\\";
        this.lineEnding = "\r\n";
        this.separatorSequence = ",";
        this.decimalSeparator = ".";
        this.dateFormat = "yyyy-MM-dd'T'HH:mm:ssXXX";
	}
	public String getCharSet() {
		return charSet;
	}
	public void setCharSet(String charSet) {
		this.charSet = charSet;
	}
	public String getDateFormat() {
		return dateFormat;
	}
	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}
	public String getDecimalSeparator() {
		return decimalSeparator;
	}
	public void setDecimalSeparator(String decimalSeparator) {
		this.decimalSeparator = decimalSeparator;
	}
	public String getEscapeSequence() {
		return escapeSequence;
	}
	public void setEscapeSequence(String escapeSequence) {
		this.escapeSequence = escapeSequence;
	}
	public String getLineEnding() {
		return lineEnding;
	}
	public void setLineEnding(String lineEnding) {
		this.lineEnding = lineEnding;
	}
	public char getQuoteSequence() {
		return quoteSequence;
	}
	public void setQuoteSequence(char c) {
		this.quoteSequence = c;
	}
	public String getSeparatorSequence() {
		return separatorSequence;
	}
	public void setSeparatorSequence(String separatorSequence) {
		this.separatorSequence = separatorSequence;
	}
	public String getThousandSeparator() {
		return thousandSeparator;
	}
	public void setThousandSeparator(String thousandSeparator) {
		this.thousandSeparator = thousandSeparator;
	}
	
}

