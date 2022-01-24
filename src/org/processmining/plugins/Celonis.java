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
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
	
	public void getDataPools() {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = this.url + "/integration/api/pools";
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(headers);       
        restTemplate.exchange(targetUrl, HttpMethod.GET, jobRequest, String.class);        
	}
	public void getDataModels(String dataPoolId) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = this.url + "/integration/api/pools/" + dataPoolId + "/data-models";
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(headers);       
        restTemplate.exchange(targetUrl, HttpMethod.GET, jobRequest, String.class);        
	}
	public void getAnalyses() {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = this.url + "/process-mining/api/analysis";
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(headers);       
        restTemplate.exchange(targetUrl, HttpMethod.GET, jobRequest, String.class);        
	}
	public void getAnalysesDataModel(String anaId) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = this.url + "/process-mining/analysis/v1.2/api/analysis/" + anaId + "/data_model";
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(headers);       
        restTemplate.exchange(targetUrl, HttpMethod.GET, jobRequest, String.class);        
	}
	
	public String createAnalysis(String workspaceId, String name) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = this.url + "/process-mining/api/analysis";
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        JSONObject request = new JSONObject();
        request.put("name", name);
        request.put("processId", workspaceId);
        request.put("applicationId", JSONObject.NULL);
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(request.toString(), headers);       
        
        ResponseEntity<String> r = restTemplate.exchange(targetUrl, HttpMethod.POST, jobRequest, String.class);
        
        JSONObject body = new JSONObject(r.getBody());
        return body.getString("id");
	}
	
	public String createWorkspace (String dataModelId, String name) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = this.url + "/process-mining/api/processes";
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        JSONObject request = new JSONObject();
        request.put("name", name);
        request.put("dataModelId", dataModelId);
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(request.toString(), headers);       
        
        ResponseEntity<String> r = restTemplate.exchange(targetUrl, HttpMethod.POST, jobRequest, String.class);
        
        JSONObject body = new JSONObject(r.getBody());
        return body.getString("id");
        
	}
	
	public void addProcessConfiguration (String dataModelId, String dataPoolId, String actTable, String caseTable, String caseIdColumn, 
			String actColumn, String timestampColumn) {
		String actTableId = this.getTableByName(actTable, dataModelId, dataPoolId);
		String caseTableId = this.getTableByName(caseTable, dataModelId, dataPoolId);
		
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = String.format(this.url + "/integration/api/pools/%s/data-models/" + dataModelId + "/process-configurations", dataPoolId);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        JSONObject request = new JSONObject();
        request.put("activityTableId", actTableId);
        request.put("caseTableId", caseTableId);
        request.put("caseIdColumn", caseIdColumn);
        request.put("activityColumn", actColumn);
        request.put("timestampColumn", timestampColumn);
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(request.toString(), headers);        
        restTemplate.exchange(targetUrl, HttpMethod.PUT, jobRequest, String.class);          
        
	}
	
	public void reloadDataModel (String dataModelId, String dataPoolId) throws InterruptedException {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = String.format(this.url + "/integration/api/pools/%s/data-models/" + dataModelId + "/reload", dataPoolId);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        JSONObject request = new JSONObject();
        request.put("forceComplete", true);
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(request.toString(), headers);        
        restTemplate.postForEntity(targetUrl, jobRequest, Object.class);        
        
        while(true) {
        	String loadUrl = String.format(this.url + "/integration/api/pools/%s/data-models/" + 
        						dataModelId + "/load-history/load-info-sync", dataPoolId);
        	HttpEntity<String> loadRequest = new HttpEntity<String>(null,headers);  
        	RestTemplate restTemplate1 = new RestTemplate();
    		ResponseEntity<String> r = restTemplate1.exchange(loadUrl, HttpMethod.GET, loadRequest, String.class);
    		
    		JSONObject body = new JSONObject(r.getBody());
    		String status = body.getJSONObject("loadInfo").getJSONObject("currentComputeLoad").getString("loadStatus");
    		if (status.equals("SUCCESS")) {
    			break;
    		}
    		TimeUnit.SECONDS.sleep(3);
    		
        }
	}
	
	public String getTableByName(String tableName, String dataModelId, String dataPoolId) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = String.format(this.url + "/integration/api/pools/%s/data-model/" + dataModelId + "/tables", dataPoolId);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        HttpEntity<Void> jobRequest = new HttpEntity<>(headers);
        
        ResponseEntity<String> r = restTemplate.exchange(targetUrl, HttpMethod.GET, jobRequest, String.class);
        JSONArray body = new JSONArray(r.getBody());
        
        String res = "";
        for (int i = 0; i < body.length(); i++) {
        	JSONObject obj = body.getJSONObject(i);        	
        	if (obj.getString("name").equals(tableName)) {        		
        		res = obj.getString("id");
        		break;
        	}
        }
        
        return res;
        
	}
	
	public void addForeignKeys(String table1, String column1, String table2, String column2, String dataModelId, String dataPoolId) {
		String table1Id = this.getTableByName(table1, dataModelId, dataPoolId);
		String table2Id = this.getTableByName(table2, dataModelId, dataPoolId);
		
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = String.format(this.url + "/integration/api/pools/%s/data-models/" + dataModelId + "/foreign-keys", dataPoolId);
		HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);		
        
        JSONObject request = new JSONObject();
        JSONObject column = new JSONObject();
        column.put("sourceColumnName", column1);
        column.put("targetColumnName", column2);
        JSONArray columns = new JSONArray();
        columns.put(column);
        request.put("dataModelId", dataModelId);
        request.put("sourceTableId", table1Id);
        request.put("targetTableId", table2Id);
        request.put("columns", columns);
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(request.toString(), headers);        
        restTemplate.postForEntity(targetUrl, jobRequest, Object.class);        
        
 	}
	
	public void addTableFromPool(String tableName, String dataPoolId, String dataModelId) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = String.format(this.url + "/integration/api/pools/%s/data-model/" + dataModelId + "/tables", dataPoolId);
		HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);		
        
        JSONObject request = new JSONObject();
        request.put("name", tableName);
        request.put("alias", tableName);
        request.put("dataModelId", dataModelId);
        request.put("dataSourceId", JSONObject.NULL);
        
        List<JSONObject> request1 = new ArrayList<JSONObject>();
        request1.add(request);
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(request1.toString(), headers);        
       
        restTemplate.postForEntity(targetUrl, jobRequest, Object.class);        
	}
	
	public String createDataPool(String name) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = this.url + "/integration/api/pools";
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);	
        
        JSONObject request = new JSONObject();
        request.put("name", name);
        
        HttpEntity<String> jobRequest = new HttpEntity<String>(request.toString(), headers);       
        
        ResponseEntity<String> r = restTemplate.exchange(targetUrl, HttpMethod.POST, jobRequest, String.class);
        
        JSONObject body = new JSONObject(r.getBody());
        return body.getString("id");
	}
	
	public String createDataModel(String modelName, String dataPoolId) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = String.format(this.url + "/integration/api/pools/%s/data-models", dataPoolId);
		HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);		
        
        JSONObject request = new JSONObject();
        request.put("name", modelName);
        request.put("poolId", dataPoolId);
        request.put("configurationSkipped", true);

        HttpEntity<String> jobRequest = new HttpEntity<String>(request.toString(), headers);
 
        String r = restTemplate.postForObject(targetUrl, jobRequest, String.class);
        JSONObject body = new JSONObject(r);
        return body.getString("id");
	}
	
	public void uploadCSV(String dataPoolId, String fileLocation, String tableName, String timestampColumn, int chunkSize) throws CsvValidationException, IOException, InterruptedException, ExecutionException {
//		System.out.println("Creating table schema");
//		System.out.println("##################");
		TableTransport tableSchema = getTableConfig(fileLocation, timestampColumn, tableName);
		String jobId = this.createPushJob(dataPoolId, tableName, tableSchema);
		this.uploadCsvChunk(chunkSize, dataPoolId, jobId, fileLocation);
		this.executeJob(jobId, dataPoolId);	
		System.out.println("start getting status");
		
		while (true) {
			String status = getStatus(dataPoolId, jobId);
			System.out.println(status);
			if (status.equals("DONE")){
				break;
			}
			TimeUnit.SECONDS.sleep(3);
		}
		System.out.println("done");

	}
	
	private String getStatus(String dataPoolId, String jobId) {
		RestTemplate restTemplate = new RestTemplate();
		String targetUrl = String.format(this.url + "/integration/api/v1/data-push/%s/jobs/" + jobId, dataPoolId);
		HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + this.apiToken);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Void> jobRequest = new HttpEntity<>(headers);
		ResponseEntity<String> r = restTemplate.exchange(targetUrl, HttpMethod.GET, jobRequest, String.class);
		JSONObject body = new JSONObject(r.getBody());
		return body.getString("status");
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
        Object re = restTemplate.postForEntity(sealUrl, sealRequest, Object.class);    
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
