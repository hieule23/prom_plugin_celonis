package org.processmining.plugins;



import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import com.opencsv.CSVWriter;

public class CSVUtils {
	static void createActCSV (XLog log, File output) throws Exception {
		
		
		Map<String, Set<String>> keyDict = new HashMap<String, Set<String>>();
		
		Set<String> keyEvent = new HashSet<String>();
		Set<String> keyTrace = new HashSet<String>();
		
		for (XTrace trace : log) {
			Set<String> key_trace = trace.getAttributes().keySet();
			if (!keyTrace.containsAll(key_trace)) {
				key_trace.removeAll(keyTrace);
				for (String tr: key_trace) {
					String add = "case:" + tr;
					keyTrace.add(add);
				}
			}
		}	
		for (XAttribute att : log.getGlobalEventAttributes()) {
			keyEvent.add(att.getKey());
		}
		keyDict.put("keyEvent", keyEvent);
		keyDict.put("keyTrace", keyTrace);
		FileWriter outputFile = new FileWriter(output);
		CSVWriter writer = new CSVWriter(outputFile);	
		List<String> header = new ArrayList<>();
		for (String key: keyDict.keySet()) {
			header.addAll(keyDict.get(key));
		}
		String[] r = header.toArray(new String[header.size()]);
		
		writer.writeNext(r);
		System.out.println(header);
		for (XTrace trace: log) {		
			for (XEvent event: trace) {
				List<Object> line = new ArrayList<>();
				for (String key: keyDict.get("keyTrace")) {
					key = key.substring(5);
					if (trace.getAttributes().get(key) == null) {
						line.add("");
					}
					else {
						line.add(trace.getAttributes().get(key).toString());
					}
				}
				for (String key: keyDict.get("keyEvent")) {		
					line.add(event.getAttributes().get(key).toString());
				}
				String[] line_add = line.toArray(new String[line.size()]);
				writer.writeNext(line_add);
			}
			
		}
		writer.close();
		
	}
	
	static void createCaseCSV(XLog log, File output) throws Exception {
		FileWriter outputFile = new FileWriter(output);
		CSVWriter writer = new CSVWriter(outputFile);	
		String [] header = new String[] {"case:concept:name"};
		writer.writeNext(header);
		List<String> listCase = new ArrayList<String>();
		for (XTrace trace : log) {
			if (!listCase.contains(trace.getAttributes().get("concept:name").toString())) {
				listCase.add(trace.getAttributes().get("concept:name").toString());
				String [] line = new String[] {trace.getAttributes().get("concept:name").toString()};
				writer.writeNext(line);				
			}
		}		
		writer.close();
	}
}






























