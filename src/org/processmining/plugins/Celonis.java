package org.processmining.plugins;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.support.AllEncompassingFormHttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

public class Celonis {
	private String url;
	private String apiToken;
	
	
	public Celonis (String url, String apiToken) {
		this.url = url;
		this.apiToken = apiToken;
	}
	
	public void uploadCSV(String dataPoolId, String fileLocation, String tableName, String timestampColumn, int chunkSize) throws CsvValidationException, IOException, InterruptedException, ExecutionException {
//		System.out.println("Creating table schema");
//		System.out.println("##################");
		TableTransport tableSchema = this.getTableConfig(fileLocation, timestampColumn, tableName);
		String jobId = this.createPushJob(dataPoolId, tableName, tableSchema);
		this.uploadCsvChunk(chunkSize, dataPoolId, jobId, fileLocation);
		this.executeJob(jobId, dataPoolId);
		        
	}
	
	private String createPushJob(String dataPoolId, String tableName, TableTransport tableSchema) {		
        DataPushJob job = new DataPushJob();       
        CSVParsingOptions csvOption = new CSVParsingOptions();
        job.setDataPoolId(dataPoolId);
        job.setType(DataPushJob.type.REPLACE);
        job.setFileType(DataPushJob.fileType.CSV);
        job.setTargetName(tableName);        
        job.setUpsertStrategy(DataPushJob.upsertStrategy.UPSERT_WITH_NULLIFICATION);        
        job.setTableSchema(tableSchema);
        job.setFallbackVarcharLength(80);        
        job.setCsvParsingOptions(csvOption);
        
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);
 
        HttpEntity<DataPushJob> jobRequest = new HttpEntity<>(job, headers);
        String targetUrl = String.format(this.url + "/integration/api/v1/data-push/%s/jobs/", dataPoolId);
        // Prepare HTTP POST
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setMessageConverters(getJsonMessageConverters());
        job = restTemplate.postForObject(targetUrl, jobRequest, DataPushJob.class);
        return job.getId();
	}
	
	private void uploadCsvChunk(int chunkSize, String dataPoolId, String jobId, String fileLocation) throws CsvValidationException, IOException, InterruptedException, ExecutionException {
		System.out.println("Start devide chunks");
		long startDivide = System.currentTimeMillis();
		List<String> chunksLocation = devideChunks(fileLocation, chunkSize);
		System.out.println("Done devide chunks");
		long endDivide = System.currentTimeMillis();
		System.out.println((endDivide - startDivide)/60000);
		
		ExecutorService executorService = Executors.newFixedThreadPool(100);
		Set<Callable<String>> callables = new HashSet<Callable<String>>();
		System.out.println("Start push chunks");
		long startPush = System.currentTimeMillis();
		for (String chunk : chunksLocation) {
			callables.add(new Callable<String>() {
			    public String call() throws Exception {
			    	uploadFile(dataPoolId, jobId, chunk);
			        return "finish uploading " + chunk;
			    }
			});
		}
		List<Future<String>> futures = executorService.invokeAll(callables);

		for(Future<String> future : futures){
		    System.out.println(future.get());
		}
		executorService.shutdown();
		long endPush = System.currentTimeMillis();
		System.out.println("End"
				+ " push chunks");
		System.out.println((endPush - startPush)/60000);
		
		
		for (String chunk: chunksLocation) {
			File tempFile = new File(chunk);
			tempFile.delete();
		}
	}
//	private void uploadCsvChunk(int chunkSize, String dataPoolId, String jobId, String fileLocation) throws CsvValidationException, IOException {
//		CSVReader reader = new CSVReader(new FileReader(fileLocation));
//		String[] tableHeader = reader.readNext();
//		int chunkIndex = 0;		
//		List<String []> subLog = new ArrayList<>();
//		String[] nextLine;
//		long startPush = System.currentTimeMillis();
//		while ((nextLine = reader.readNext()) != null) {
//			if (chunkIndex != chunkSize) {				
//				subLog.add(nextLine);						
//				chunkIndex += 1;
//			}
//			else {			
//				File tempFile = File.createTempFile("chunk", ".csv");
//				System.out.println(tempFile.toPath().toString());
//				FileWriter outputFile = new FileWriter(tempFile);
//				CSVWriter writer = new CSVWriter(outputFile);
//				writer.writeNext(tableHeader);
//				writer.writeAll(subLog);
//				writer.close();
//				this.uploadFile(dataPoolId, jobId, tempFile.toPath().toString());		
//				tempFile.delete();
//				chunkIndex = 1;				
//				subLog.clear();
//				subLog.add(nextLine);				
//			}			
//		}
//		if (chunkIndex != 0) {
//			File tempFile = File.createTempFile("chunk", ".csv");
//			System.out.println(tempFile.toPath().toString());
//			FileWriter outputFile = new FileWriter(tempFile);
//			CSVWriter writer = new CSVWriter(outputFile);
//			writer.writeNext(tableHeader);
//			writer.writeAll(subLog);
//			writer.close();
//			this.uploadFile(dataPoolId, jobId, tempFile.toPath().toString());	
//		}
//		long endPush = System.currentTimeMillis();	
//		System.out.println((endPush - startPush)/60000);
//		
//	}
	
	private List<String> devideChunks(String fileLocation, int chunkSize) throws CsvValidationException, IOException {
		List<String> chunksLocation = new ArrayList<String>();
		
		CSVReader reader = new CSVReader(new FileReader(fileLocation));
		String[] tableHeader = reader.readNext();
		int chunkIndex = 0;		
		List<String []> subLog = new ArrayList<>();
		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			if (chunkIndex != chunkSize) {				
				subLog.add(nextLine);						
				chunkIndex += 1;
				System.out.println(nextLine[1]);
				System.out.println(chunkIndex);
			}
			else {
				System.out.println(subLog.get(4)[1]);				
				File tempFile = File.createTempFile("chunk", ".csv");
				System.out.println(tempFile.toPath().toString());
				FileWriter outputFile = new FileWriter(tempFile);
				CSVWriter writer = new CSVWriter(outputFile);
				writer.writeNext(tableHeader);
				writer.writeAll(subLog);
				writer.close();
				chunksLocation.add(tempFile.toString());
				chunkIndex = 1;				
				subLog.clear();
				subLog.add(nextLine);				
			}			
		}
		if (chunkIndex != 0) {
			File tempFile = File.createTempFile("chunk", ".csv");
			System.out.println(tempFile.toPath().toString());
			FileWriter outputFile = new FileWriter(tempFile);
			CSVWriter writer = new CSVWriter(outputFile);
			writer.writeNext(tableHeader);
			writer.writeAll(subLog);
			writer.close();
			chunksLocation.add(tempFile.toString());
		}
		
		return chunksLocation;
	}
	
	private void uploadFile(String dataPoolId, String jobId, String fileLocation) {
		String pushUrl = String.format(this.url + "/integration/api/v1/data-push/%s/jobs/" + jobId + "/chunks/upserted", dataPoolId);
        LinkedMultiValueMap<String, Object> requestMap = new LinkedMultiValueMap<String, Object>();       
        requestMap.add("file", new FileSystemResource(fileLocation));          
        
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        
        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(requestMap, headers);        
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setMessageConverters(getFormMessageConverters());
        restTemplate.postForEntity(pushUrl, requestEntity, Object.class);
	}
	
	private void executeJob(String jobId, String dataPoolId) {
		String sealUrl = String.format(this.url + "/integration/api/v1/data-push/%s/jobs/" + jobId, dataPoolId);
		
		HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);
        
		HttpEntity<Object> sealRequest = new HttpEntity<>(null, headers);
		RestTemplate restTemplate = new RestTemplate();
        restTemplate.setMessageConverters(getJsonMessageConverters());
        restTemplate.postForEntity(sealUrl, sealRequest, Object.class);    
	}
	
	private static TableTransport getTableConfig(String fileLocation, String timestampColumn, String tableName) throws CsvValidationException, IOException {
		CSVReader reader = new CSVReader(new FileReader(fileLocation));
	    String [] tableHeader = reader.readNext();
		TableTransport tableSchema = new TableTransport();
		ColumnTransport[] tableCol = new ColumnTransport[tableHeader.length];
		
		for (int i = 0; i < tableHeader.length; i++) {			
			if (tableHeader[i].equals(timestampColumn)) {
				ColumnTransport column = new ColumnTransport();
				column.setColumnName(tableHeader[i]);
				column.setColumnType(ColumnTransport.columnType.DATETIME);
				tableCol[i] = column;
			} 
			else {
				ColumnTransport column = new ColumnTransport();
				column.setColumnName(tableHeader[i]);
				column.setColumnType(ColumnTransport.columnType.STRING);
				tableCol[i] = column;
			}
			
		}
		tableSchema.setColumns(tableCol);
		tableSchema.setTableName(tableName);
		return tableSchema;
	}
	
	private static List<HttpMessageConverter<?>> getJsonMessageConverters() {
	    List<HttpMessageConverter<?>> converters = new ArrayList<>();
	    converters.add(new MappingJackson2HttpMessageConverter());
	    return converters;
	}
	
	private static List<HttpMessageConverter<?>> getFormMessageConverters() {
	    List<HttpMessageConverter<?>> converters = new ArrayList<>();
	    converters.add(new AllEncompassingFormHttpMessageConverter());
	    return converters;
	}
	
	
}
