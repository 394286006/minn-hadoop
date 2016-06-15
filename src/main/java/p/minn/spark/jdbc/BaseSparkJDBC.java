package p.minn.spark.jdbc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import p.minn.common.utils.Page;

/**
 * 
 * @author minn
 * @QQ:3942986006
 * @omment 
 */
public class BaseSparkJDBC<T> implements Serializable{

 
  private JavaSparkContext javaSparkContext;
  
  private SQLContext sqlContext;
  
  private Properties options;
  
  
  
  public BaseSparkJDBC(String url,String driver,String user,String password) {
    super();
    options=new Properties();
    options.put("url", url);
    options.put("driver", driver);
    options.put("user", user);
    options.put("password", password);
  }

  public void save(List<T> list,Class<T> clz,String targettable){
    JavaRDD<T> jrdd=  javaSparkContext.parallelize(list);
    DataFrame df=sqlContext.createDataFrame(jrdd,clz);
    df.write().mode("append").jdbc(options.getProperty("url"), targettable, options);
  }

  public void save(T hs,Class<T> clz,String targettable) {
    List<T> list=new ArrayList<T>();
    list.add(hs);
    save(list,clz,targettable);
  }
  
  
  protected  List<T> pageSql(Function<Row,T> rdd,Page page,String targettable,String sqltxt) throws Exception{
    DataFrame jdbcDF =sqlContext.read().jdbc(options.getProperty("url"), targettable, options);
    jdbcDF.registerTempTable(targettable);
    List<T> list=jdbcDF.sqlContext().sql(sqltxt).limit(page.getTotal()).javaRDD().map(rdd).take(page.getRp());
    return list;
  }

  public  int getTotal(String targettable,String sqltxt){
    int count=0;
    DataFrame jdbcDF =sqlContext.read().jdbc(options.getProperty("url"), targettable, options);
    jdbcDF.registerTempTable(targettable);
    Row[] rows= jdbcDF.sqlContext().sql(sqltxt).collect();
    if(rows!=null){
      count=(int)rows[0].getLong(0);
    }
    return count;
  }
  public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
    this.javaSparkContext = javaSparkContext;
  }


  public void setSqlContext(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

}
