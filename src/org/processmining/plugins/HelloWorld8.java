package org.processmining.plugins;

import java.io.File;
import java.io.IOException;

import org.deckfour.xes.model.XLog;
import org.processmining.contexts.uitopia.UIPluginContext;
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.annotations.Plugin;
import org.processmining.log.csvexport.ExportLogCsv;

import com.opencsv.exceptions.CsvValidationException;




public class HelloWorld8 {
    @Plugin(
            name = "aaaa", 
            parameterLabels = { "Log"}, 
            returnLabels = { "First string several second strings" }, 
            returnTypes = { XLog.class }, 
            userAccessible = true, 
            help = "Produces one string consisting of the first and a number of times a string given as input in a dialog."
    )
    @UITopiaVariant(
            affiliation = "rwth", 
            author = "Hieu Le", 
            email = "hieu.le@rwth-aachen.de"
    )
    public static XLog helloWorlds(UIPluginContext context, XLog log) throws IOException, CsvValidationException{
    	ExportLogCsv export = new ExportLogCsv();
    	String tempFileLocation = "D:\\eclipse\\proj\\test\\data\\new-test.csv";
    	File csv = new File(tempFileLocation);
    	export.export(context, log, csv);
    	
    	
    	String url = "https://hieu-le-rwth-aachen-de.training.celonis.cloud";
    	String apiToken = "M2M5ZWJmODItZTYzZi00ZDI5LTg3Y2QtYjY2NWY4YmFkOWY2Om9HKytLY1lZcFV6NXNSSnBGS0hNc2poUUJ0R09sbXNjTnp5Syt6VDVkZDZP";
    	String dataPoolId = "1956bd42-c2ab-4d0a-a0ba-85346953ffa0";
    	String timestampName = "completeTime";
    	String tableName = "new-csv-6";
    	
    	Celonis celonis = new Celonis(url, apiToken);
    	
        
    	celonis.uploadCSV(dataPoolId, tempFileLocation, tableName, timestampName, 5);    
        return log;            
        
        
    }

}


