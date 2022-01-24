package org.processmining.plugins;

import java.io.Serializable;

public class DataPushJob implements Serializable{
	private String connectionId;
	private CSVParsingOptions csvParsingOptions;
	private String dataPoolId;
	private int fallbackVarcharLength;	
	private fileType fileType;
	private String[] keys;
	private TableTransport tableSchema;
	private String targetName;
	private type type;
	private upsertStrategy upsertStrategy;	
	public String id;
	
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	public String getConnectionId() {
		return connectionId;
	}
	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
	public CSVParsingOptions getCsvParsingOptions() {
		return csvParsingOptions;
	}
	public void setCsvParsingOptions(CSVParsingOptions csvParsingOptions) {
		this.csvParsingOptions = csvParsingOptions;
	}
	public String getDataPoolId() {
		return dataPoolId;
	}
	public void setDataPoolId(String dataPoolId) {
		this.dataPoolId = dataPoolId;
	}
	public int getFallbackVarcharLength() {
		return fallbackVarcharLength;
	}
	public void setFallbackVarcharLength(int fallbackVarcharLength) {
		this.fallbackVarcharLength = fallbackVarcharLength;
	}
	public fileType getFileType() {
		return fileType;
	}
	public void setFileType(fileType fileType) {
		this.fileType = fileType;
	}
	public String[] getKeys() {
		return keys;
	}
	public void setKeys(String[] keys) {
		this.keys = keys;
	}
	public TableTransport getTableSchema() {
		return tableSchema;
	}
	public void setTableSchema(TableTransport tableSchema) {
		this.tableSchema = tableSchema;
	}
	public String getTargetName() {
		return targetName;
	}
	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}
	public type getType() {
		return type;
	}
	public void setType(type type) {
		this.type = type;
	}
	public upsertStrategy getUpsertStrategy() {
		return upsertStrategy;
	}
	public void setUpsertStrategy(upsertStrategy upsertStrategy) {
		this.upsertStrategy = upsertStrategy;
	}
	public enum fileType{
		PARQUET, CSV
	}
	public enum type{
		REPLACE, DELTA
	}
	public enum upsertStrategy{
		UPSERT_WITH_UNCHANGED_METADATA, UPSERT_WITH_NULLIFICATION
	}
}
