package p.minn.jdbc.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import p.minn.common.utils.Page;
import p.minn.hadoop.entity.HadoopSpark;
import p.minn.jdbc.map.MapHadoopHiveFun;

/**
 * 
 * @author minn
 * @QQ:3942986006
 * @omment 
 */
public class HadoopHiveJDBC extends BaseHiveJDBC<HadoopSpark>{

  

  public List<HadoopSpark> query(Connection conn,Page page,String sqltxt) {
    
    Function<ResultSet,HadoopSpark> hive=new MapHadoopHiveFun();
    List<HadoopSpark> list;
    try {
      list =super.pageSql(conn,hive,sqltxt+" limit "+page.getStartR()+","+page.getRp());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace(); 
     throw new RuntimeException(e.getMessage());
    }
    
    return list;
  }
  
}
