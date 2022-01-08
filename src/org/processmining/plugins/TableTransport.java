package org.processmining.plugins;

public class TableTransport {
	private ColumnTransport[] columns;
	private String tableName;
	
	public ColumnTransport[] getColumns() {
		return columns;
	}
	public void setColumns(ColumnTransport[] columns) {
		this.columns = columns;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	
}
