# hive-custom-hook
Example on how to implement a hive hook

## Build jar with dependencies
```bash
mvn assembly:assembly
```

## Register your hook in Hive (Permanently, stays even if you restart)
```bash
# Open the file
vi /opt/hive/conf/hive-env.sh

# At the end of the file, add the line and save:
export HIVE_AUX_JARS_PATH=/hive-custom-hook-1.0-SNAPSHOT-jar-with-dependencies.jar

# Open the file
vi /opt/hive/conf/hive-site.xml

# Add the tag before the closing configuration tag </configuration> and save:
<property><name>hive.exec.post.hooks</name><value>com.medium.hive.hook.CustomHook</value></property>

# Restart your Hive server
```

## Register your hook in Hive Manually (For testing purposes, if your restart its cleared)
```bash
# Inside your Hive shell, run

ADD JAR /hive-custom-hook-1.0-SNAPSHOT-jar-with-dependencies.jar
set hive.exec.post.hooks=com.medium.hive.hook.CustomHook;

```
