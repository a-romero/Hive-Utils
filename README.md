# Hive-Utils
Additional utilities for Hive

# Hive Hook for queue mapping

The generated library will allow queue mapping based on a configuration "group-mappings" file. This solves the issue when running Hive With doAs=false that YARN queue mappings are not applied due to jobs being run as user 'hive'. This Hive hook is able to detect the user that started the hive session, find the groups that it belongs to, and send the job to the corresponding queue depending on that group and the mappings we define on the "group-mappings" file.

1. On all HiveServer2 servers do:
mkdir /usr/hdp/current/hive-client/auxlib/
wget https://github.com/beto983/Hive-Utils/blob/master/Hive-Utils-1.0-jar-with-dependencies.jar -O /usr/hdp/current/hive-client/auxlib/Hive-Utils-1.0-jar-with-dependencies.jar

2. Add the following setting on hive-site.xml (Custom hiveserver2-site on Ambari): 
hive.semantic.analyzer.hook=com.github.beto983.hive.hooks.YARNQueueHook

3. Create a "group-mappings" file in /etc/hive/conf/ with the structure:

          groupname:queuename
          groupname:queuename
          groupname:queuename
          ...

4. Restart Hive



