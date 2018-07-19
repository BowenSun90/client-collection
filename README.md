[![GitHub Release](https://github.com/BowenSun90/Picture-resources/blob/master/license.jpeg)](https://github.com/BowenSun90/Springboot-mybatis-multi-datasource)

# Collection Utils
Various set of util project


## Hadoop
配置Hadoop配置文件路径和kerberos验证信息 `resources/application.properties`
```
# Hadoop configuration root path
# ${hadoop.conf.path}/hdfs-site.xml
# ${hadoop.conf.path}/core-site.xml
# ${hadoop.conf.path}/mapred-site.xml
# ${hadoop.conf.path}/yarn-site.xml
hadoop.conf.path=${hadoop.conf.path}

# support kerberos
hadoop.security.authentication=${hadoop.conf.path}
username.client.kerberos.principal=${hadoop.conf.path}
username.client.keytab.file=${hadoop.conf.path}
java.security.krb5.conf=${hadoop.conf.path}
```

**1.操作Hdfs文件和文件夹**  
[com.alex.space.hadoop.utils.HdfsUtils](https://github.com/BowenSun90/Utils/blob/master/hadoop-client/src/main/java/com/alex/space/hadoop/utils/HdfsUtils.java)


**2.MapReduce示例**  
- 2.1 排序   
  - 2.1.1 简单排序   
  [com.alex.space.hadoop.example.simplesort](https://github.com/BowenSun90/Utils/blob/master/hadoop-client/src/main/java/com/alex/space/hadoop/example/simplesort)   
  - 2.1.2 二次排序      
  [com.alex.space.hadoop.example.secondsort](https://github.com/BowenSun90/Utils/blob/master/hadoop-client/src/main/java/com/alex/space/hadoop/example/secondsort)    
- 2.2 比较
  - 2.2.1 按值排序  
  [com.alex.space.hadoop.example,comparator](https://github.com/BowenSun90/Utils/blob/master/hadoop-client/src/main/java/com/alex/space/hadoop/example/comparator)   


## HBase Client
配置HBase连接信息 `resources/application.properties`
```
# HBase connection
hbase.zookeeper.quorum=${hbase.zookeeper.quorum}
hbase.zookeeper.property.clientPort=${hbase.zookeeper.property.clientPort}

# Hadoop and Hbase configuration root path
# ${hadoop.conf.path}/hdfs-site.xml
# ${hadoop.conf.path}/core-site.xml
# ${hbase.conf.path}/hbase-site.xml
hadoop.conf.path=${hadoop.conf.path}
hbase.conf.path=${hbase.conf.path}

# support kerberos
hadoop.security.authentication=none
username.client.kerberos.principal=
username.client.keytab.file=
```

**1.HBaseAdmin操作HBase**  
[com.alex.space.hbase.utils.HBaseAdminUtils](https://github.com/BowenSun90/Utils/blob/master/hbase-client/src/main/java/com/alex/space/hbase/utils/HbaseAdminUtils.java)  


**2.HBaseAPI操作HBase**  
[com.alex.space.hbase.utils.HBaseUtils](https://github.com/BowenSun90/Utils/blob/master/hbase-client/src/main/java/com/alex/space/hbase/utils/HBaseUtils.java)  
配置HBase连接信息 `resources/application.properties`


**3.创建预分区表**  
[com.alex.space.hbase.utils.HBaseTableUtils](https://github.com/BowenSun90/Utils/blob/master/hbase-client/src/main/java/com/alex/space/hbase/utils/HBaseTableUtils.java)  
根据Rowkey首字母分区，0\~9A\~Za~z共62个region
>建议Rowkey添加md5前缀，如果不需要scan一个区域的功能



## ElasticSearch
配置ES连接信息 `resources/application.properties`
```
# elastic connection info
es.cluster.name=${es.cluster.name}
es.node.ip=${es.node.ip}
es.node.port=${es.node.port}
```

**1.TransportClient操作Elastic**    
[com.alex.space.elastic.utils.ElasticUtils](https://github.com/BowenSun90/Utils/blob/master/elastic-client/src/main/java/com/alex/space/elastic/utils/ElasticUtils.java)




## Zookeeper
配置ZK连接信息 `resources/application.properties`
```
# zookeeper connection info
zk.nodes=${zk.nodes}
```

**1.ZKClient操作Zookeeper**  
[com.alex.space.zoo.client.ZooClient](https://github.com/BowenSun90/Utils/blob/master/zoo-client/src/main/java/com/alex/space/zoo/client/ZkClientDemo.java)


**2.Curator操作Zookeeper**  
[com.alex.space.zoo.curator.CuratorClientDemo](https://github.com/BowenSun90/Utils/blob/master/zoo-client/src/main/java/com/alex/space/zoo/curator/CuratorClientDemo.java)


**3.Curator高级特性**  
[com.alex.space.zoo.curator.recipes](https://github.com/BowenSun90/Utils/blob/master/zoo-client/src/main/java/com/alex/space/zoo/curator/recipes)



## Spark
修改`application.properties`，设置`env=local`本地执行，`env=prod`生产集群执行   
本地执行配置（如文件路径）在`local.properties`中配置   
集群执行配置（如文件路径）在`prod.properties`中配置  

**1.Spark任务基类**   
[com.alex.space.spark.mains.BaseWorker](https://github.com/BowenSun90/client-collection/blob/master/spark-client/src/main/scala/com/alex/space/spark/mains/BaseWorker.scala)  
默认的配置以`default`作为前缀   
```
# application.conf
env=local
# local.conf
default.input=/home/hadoop/test
# com.alex.space.spark.mains.Worker
  ...
  println("input:" + configString("input"))
  ...
```
一个新的任务，继承BaseWorker，重写prefix，可以读取对应prefix的配置
```
# xxx.conf
test.input=/home/hadoop/example

# XXXWorker
override val prefix: String = "test"
println("input:" + configString("input"))

# input:/home/hadoop/example
```


**2.Spark SQL与Hive互操作**   
[com.alex.space.spark.mains.DataFrameDemo](https://github.com/BowenSun90/client-collection/blob/master/spark-client/src/main/scala/com/alex/space/spark/mains/DataFrameDemo.scala)


**3.Livy通过Rest提交Spark任务**
[com.alex.space.spark.livy](https://github.com/BowenSun90/client-collection/blob/master/spark-client/src/main/java/com/alex/space/spark/livy)
提交方式与[spark-client/bin/wordCount.sh](https://github.com/BowenSun90/client-collection/blob/master/spark-client/bin/wordCount.sh)相同


## Hive
**1.hive常用udf方法**  
[com.alex.space.hive.udf](https://github.com/BowenSun90/client-collection/blob/master/hive-udf/src/main/java/com/alex/space/hive/udf)

## Storm
**1.Storm接入Kafka数据**  
== TODO ==






TO be continue
---
