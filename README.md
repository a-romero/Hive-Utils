# Hive-Utils
Additional utilities for Hive

# Hive Hook for queue mapping based on config file

1. On all HiveServer2 servers do:
mkdir /usr/hdp/current/hive-client/auxlib/
wget https://github.com/beto983/Hive-Utils/blob/master/Hive-Utils-1.0-jar-with-dependencies.jar -O /usr/hdp/current/hive-client/auxlib/Hive-Utils-1.0-jar-with-dependencies.jar

2. Add the following setting on hive-site.xml (Custom hiveserver2-site on Ambari): 
hive.semantic.analyzer.hook=com.github.beto983.hive.hooks.YARNQueueHook

3. Restart Hive



