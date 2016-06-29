package p.minn.hive.jdbc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.hive.HiveContext;



import scala.Function1;
import scala.Option;
import scala.PartialFunction;
import scala.collection.Iterator;
import scala.reflect.ClassManifestFactory;
import scala.runtime.BoxedUnit;

public class HiveDBC implements Serializable {

  public static void main(String[] args) throws Exception {
	  //testHiveSpark();
	  testHive();
}
  public static void testHiveSpark(){
	  Map<String, String> options = new HashMap<String, String>();
	    options.put("url","jdbc:mysql://192.168.8.104:3306/test?useUnicode=true&characterEncoding=utf-8");
	    options.put("dbtable", "hadoopspark");
	    options.put("driver", "com.mysql.jdbc.Driver");
	    options.put("user", "root");
	    options.put("password", "123456");
	//spark://namenode1:7077 local[*]
	    JavaSparkContext sc=new JavaSparkContext(new SparkConf().setAppName("DBConnection").setMaster("spark://namenode1:7077"));
	    sc.addJar("/usr/local/spark/examples/hadoopspark.jar");
	    sc.addJar("/usr/local/spark/spark-extends-lib/mysql-connector-java-5.1.38.jar");
	    sc.addJar("/usr/local/hive/lib/hive-jdbc-2.0.1-standalone.jar");
	    HiveContext sqlContext = new HiveContext(sc);
	    Properties prop=new Properties();
	    prop.put("url","jdbc:hive2://localhost:10000");
	    //prop.put("dbtable", "default.test1");
		prop.put("user", "root");
		prop.put("password", "123456");
		prop.put("driver", "org.apache.hive.jdbc.HiveDriver");
	  //jdbcDF.registerTempTable("hadoopspark");
		
	//  DataFrame df= sqlContext.read().jdbc("jdbc:hive2://localhost:10000/default", "default.test1", prop);
	 // df.registerTempTable("test1");
		sqlContext.setConf(prop);
	
	//sqlContext.read().format("jdbc").options(options).load();
	
	//sqlContext.sql("use default");	
	//sqlContext.sql("drop TABLE test2");
	//sqlContext.refreshTable("test1");
		sqlContext.refreshTable("default");
	Row[] rs=sqlContext.sql("from test1 select id,name").collect();
	System.out.println("show start:"+rs.length);
	for(Row r:rs){
		System.out.println(r.getString(0));
	}
	System.out.println("show end");
	if(1==1)
		return;

	//sqlContext.sql("CREATE TABLE test2 (id INT, name varchar(50))");
	
	//sqlContext.sql("insert into test2(id,name) values(1,'minn1')");

	// Queries are expressed in HiveQL.
	 rs = sqlContext.sql("FROM test2 SELECT id, name").collect();
	
	System.out.println("show test2 start:"+rs.length);
	for(Row r:rs){
		System.out.println(r.getString(1));
	}
	System.out.println("show test2 end");
  }
  
  public static void testHive() {
	  try{
	  Class.forName("org.apache.hive.jdbc.HiveDriver");
	  
	  Connection conn=DriverManager.getConnection("jdbc:hive2://namenode1:10000/default","root","123456");
	  Statement stat=conn.createStatement();
	//stat.execute("CREATE TABLE hadoopspark (id int , name varchar(50)  ," +
	//		"email varchar(50)  ,qq varchar(50))");
		
   //  stat.execute("insert into hadoopspark(id,name,email,qq) values(1,'minn1','394286006@qq.com','394286006'),(2,'minn2','394286006@qq.com','394286006'),(3,'minn3','394286006@qq.com','394286006')");
    // stat=conn.createStatement(); 
     ResultSet rs=stat.executeQuery("select id,name,email,qq from hadoopspark order by id desc limit 0,2");
	 while(rs.next()){
		 System.out.println("id:"+rs.getInt("id")+",name:"+rs.getString("name"));
	 }
	  rs.close();
	  stat.close();
	  conn.close();
	  }catch(Exception e){
		  e.printStackTrace();
	  }
  }

}
