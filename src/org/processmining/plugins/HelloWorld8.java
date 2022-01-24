package org.processmining.plugins;

import java.io.File;
import java.util.Scanner;

import org.deckfour.xes.model.XLog;
import org.processmining.contexts.uitopia.UIPluginContext;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.annotations.Plugin;

public class HelloWorld8 {
    @Plugin(
            name = "aaa - Demo Celonis connection", 
            parameterLabels = { "Log"}, 
            returnLabels = { "" }, 
            returnTypes = { XLog.class }, 
            userAccessible = true, 
            help = "Connect with Celonis data pool"
    )
    @UITopiaVariant(
            affiliation = "rwth", 
            author = "Hieu Le", 
            email = "hieu.le@rwth-aachen.de"
    )
    public static XLog helloWorlds(UIPluginContext context, XLog log) throws Exception{
    	String url = "https://academic-hieu-le-rwth-aachen-de.eu-2.celonis.cloud/";
    	String apiToken = "ZTk5MGZlMjktYWU4Zi00ZTA0LWE1ZjQtYzk5OGEwMTczMDRiOlFqVmRSdlRNNmRzTmVVSzhTYm5MYW9uOG5EaHJ5bGlvT2hJSlpoYk9mVkln";
    	
    	Scanner keyboard = new Scanner(System.in);
    	System.out.println("Table name: ");
    	
    	String tableName = keyboard.nextLine();
    	
    	File actCSV = File.createTempFile("act", ".csv");
    	File caseCSV = File.createTempFile("case", ".csv");
    	CSVUtils.createActCSV(log, actCSV);
    	CSVUtils.createCaseCSV(log, caseCSV);
    	
    	
    	Celonis celonis = new Celonis(url, apiToken);    	
        String dataPoolId = celonis.createDataPool(tableName + "_DATAPOOL");
    	celonis.uploadCSV(dataPoolId, actCSV.getPath(), tableName + "_ACTIVITIES", EventLogUtils.TIMESTAMPKEY, 100000);    
    	celonis.uploadCSV(dataPoolId, caseCSV.getPath(), tableName + "_CASE", EventLogUtils.TIMESTAMPKEY, 100000);
    	String dataModelId = celonis.createDataModel(tableName + "_DATAMODEL", dataPoolId);
    	celonis.addTableFromPool(tableName + "_ACTIVITIES", dataPoolId, dataModelId);
    	celonis.addTableFromPool(tableName + "_CASE", dataPoolId, dataModelId);
    	celonis.addForeignKeys(tableName + "_ACTIVITIES", EventLogUtils.CASEIDKEY, tableName + "_CASE", 
    							EventLogUtils.CASEIDKEY, dataModelId, dataPoolId);
    	celonis.addProcessConfiguration(dataModelId, dataPoolId, tableName+"_ACTIVITIES", tableName+"_CASE", 
    								EventLogUtils.CASEIDKEY, EventLogUtils.ACTKEY, EventLogUtils.TIMESTAMPKEY);
    	celonis.reloadDataModel(dataModelId, dataPoolId);
    	String workspaceId = celonis.createWorkspace(dataModelId, tableName+"_WORKSPACE");
    	String anaId = celonis.createAnalysis(workspaceId, tableName+"_ANALYSIS");
    	
    	actCSV.delete();
    	caseCSV.delete();
    	
        return log;            
        
        
    }

}


