/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package p.minn.hadoopspark;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import p.minn.hadoop.db.Json2dbFileInputFormat;
import p.minn.hadoop.db.Json2dbMapper;
import p.minn.hadoop.db.Json2dbReducer;
import p.minn.hadoop.entity.HadoopSpark;

public class HadoopSparkJson {

  
	  private static final String DB_URL = 
			    "jdbc:mysql://192.168.8.104:3306/test?useUnicode=true&characterEncoding=utf-8";
			  private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.addResource("/etc/hadoop/core-site.xml");
    //conf.addResource("hdfs-site.xml");
    System.setProperty("HADOOP_USER_NAME", "minn");
    conf.set("fs.hdfs.impl", 
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
    conf.set("fs.file.impl",
            org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
    DBConfiguration.configureDB(conf, DRIVER_CLASS, DB_URL,"root","123456");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    Job job = Job.getInstance(conf, "json import2db");
    job.setJarByClass(HadoopSparkJson.class);
    job.setMapperClass(Json2dbMapper.class);
    job.setInputFormatClass(Json2dbFileInputFormat.class);
    job.setOutputFormatClass(DBOutputFormat.class);
    job.setReducerClass(Json2dbReducer.class);
    
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(HadoopSpark.class);
    job.setOutputValueClass(NullWritable.class);

    Json2dbFileInputFormat.addInputPath(job, new Path("input/hadoopspark.json"));
    String[] sqlname = {"name","email","qq"};
    DBOutputFormat.setOutput(job, "hadoopspark", sqlname);
   // FileOutputFormat.setOutputPath(job,
     // new Path("output"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
