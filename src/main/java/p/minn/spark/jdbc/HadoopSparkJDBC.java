package p.minn.spark.jdbc;

import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import p.minn.common.utils.Page;
import p.minn.hadoop.entity.HadoopSpark;
import p.minn.spark.map.MapHadoopSparkSFun;

/**
 * 
 * @author minn
 * @QQ:3942986006
 * @omment 
 */
public class HadoopSparkJDBC extends BaseSparkJDBC<HadoopSpark>{

  
  public HadoopSparkJDBC(String url, String driver, String user, String password) {
    super(url, driver, user, password);
    // TODO Auto-generated constructor stub
  }

  public List<HadoopSpark> query(Page page,String sqltxt) {
  
    
    Function<Row,HadoopSpark> resultrdd=new MapHadoopSparkSFun();
    List<HadoopSpark> list;
    try {
      list = super.pageSql(resultrdd,page, "hadoopspark",sqltxt);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace(); 
     throw new RuntimeException(e.getMessage());
    }
    
    return list;
  }
  
}
