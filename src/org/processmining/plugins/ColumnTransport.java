package org.processmining.plugins;

public class ColumnTransport {
	private String columnName;
	private columnType columnType;
	private int decimals;
	private int fieldLength;
	private Boolean pkField;
	
	public enum columnType{
		INTEGER, DATE, TIME, DATETIME, FLOAT, BOOLEAN, STRING
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public columnType getColumnType() {
		return columnType;
	}

	public void setColumnType(columnType columnType) {
		this.columnType = columnType;
	}

	public int getDecimals() {
		return decimals;
	}

	public void setDecimals(int decimals) {
		this.decimals = decimals;
	}

	public int getFieldLength() {
		return fieldLength;
	}

	public void setFieldLength(int fieldLength) {
		this.fieldLength = fieldLength;
	}

	public Boolean getPkField() {
		return pkField;
	}

	public void setPkField(Boolean pkField) {
		this.pkField = pkField;
	}
}
