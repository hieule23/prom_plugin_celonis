package org.processmining.plugins;
	
import org.processmining.contexts.uitopia.annotations.UITopiaVariant;
import org.processmining.framework.plugin.PluginContext;
import org.processmining.framework.plugin.annotations.Plugin;
	
	public class HelloWorld {
	        @Plugin(
	                name = "dff", 
	                parameterLabels = {}, 
	                returnLabels = { "Hello world string" }, 
	                returnTypes = { String.class }, 
	                userAccessible = true, 
	                help = "Produces the string: 'Hello world'"
	        )
	        @UITopiaVariant(
	                affiliation = "University of Mannheim", 
	                author = "Oliver Meier", 
	                email = "olivermeier92@t-online.de"
	        )
	        public static String helloWorld(PluginContext context) {
	                return "Hello World";
	        }
	}	