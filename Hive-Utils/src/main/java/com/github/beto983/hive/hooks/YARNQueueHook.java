package com.github.beto983.hive.hooks;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.IOException;
import java.util.List;

import java.nio.file.Path;
import java.util.Map;
import java.io.FileReader;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;


public class YARNQueueHook extends AbstractSemanticAnalyzerHook {

	private static final String MR_QUEUE_NAME_PROPERTY = "mapred.job.queue.name";
	private static final String TEZ_QUEUE_NAME_PROPERTY = "tez.queue.name";
	private static final String HIVE_EXECUTION_ENGINE_PROPERTY = "hive.execution.engine";

	private static final Log LOG = LogFactory.getLog(YARNQueueHook.class);
	
	@Override
	public void postAnalyze(HiveSemanticAnalyzerHookContext arg0,
			List<Task<? extends Serializable>> arg1) throws SemanticException {
	}

	@Override
	public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode node)
			throws SemanticException {
		
		//System.out.println("semantic=" + context.getUserName());
		
		HiveConf config = (HiveConf) context.getConf();
		
		//check if hive execution engine is set to tez. If so, queue name property should be tez.queue.name.
		String queue_property = MR_QUEUE_NAME_PROPERTY;
		String hiveExecEngine = config.get(HIVE_EXECUTION_ENGINE_PROPERTY);

		if(hiveExecEngine!=null && hiveExecEngine.equalsIgnoreCase("tez")){
			queue_property = TEZ_QUEUE_NAME_PROPERTY;
		}
		
		String user = context.getUserName();
		if (user == null){
			//when using beeline/jdbc client, context.getUserName() has the username
			//when using hive cli, context.getUserName() comes null and config.getUser() has the username  
			try{
				user = config.getUser();
			}catch (Exception e){
				//do nothing
				LOG.error("Error to get username" + e.toString());
			}
		}
		String allGroups = getDefaultUserGroupFromOS(user);
		
		LOG.info("YARNQueueHook found username as '" + user + "' and the list of groups from OS as '" + allGroups + "'");
		
		String queue = ReadToHashmap(allGroups);
		
		if (queue.length() > 0){
			config.set(queue_property, queue);
		}
		
		return node;
		
	}

	public static String ReadToHashmap(String group) {
        
		Map<String, String> map = new HashMap<String, String>();
		try {
        		BufferedReader in = new BufferedReader(new FileReader("/etc/hive/conf/group-mappings"));
        		String line = "";
        		while ((line = in.readLine()) != null) {
        		    String parts[] = line.split(":");
        		    map.put(parts[0], parts[1]);
        		}
        		in.close();
		}
		catch (IOException ex) {
          		ex.printStackTrace();
        	}

		String groupList[] = group.split(" ");
		for(int i=0; i<groupList.length;i++) {
			if (map.get(groupList[i]) != null) {
				String mappedQueue = map.get(groupList[i]);
				return mappedQueue;
			}	
		}
		return "";
	}

	private static String getDefaultUserGroupFromOS(String user){
		Process p;
		try {
			p = Runtime.getRuntime().exec("groups " + user);

			BufferedReader stdInput = new BufferedReader(new
					InputStreamReader(p.getInputStream()));

			String groups = stdInput.readLine();

			String groupsSplit[] = groups.split(":");

			if (groupsSplit.length == 2){
				String defaultGroup = groupsSplit[1].trim();
				return defaultGroup;
			}else{
				LOG.error("No groups found for user '" + user + "'");
				return "";
			}
		} catch (Exception e) {
			LOG.error("Error trying to groups for user '" + user + "':" + e.toString());
			return "";
		}
	}

}
