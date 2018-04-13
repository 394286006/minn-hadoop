package p.minn.jdbc.map;

import java.sql.ResultSet;

import org.apache.spark.api.java.function.Function;

import p.minn.hadoop.entity.HadoopSpark;
/**
 * 
 * @author minn
 * @QQ:3942986006
 * @omment 
 */
public class MapHadoopHiveFun implements Function<ResultSet,HadoopSpark> {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public HadoopSpark call(ResultSet row) throws Exception {
    // TODO Auto-generated method stub
    HadoopSpark hs=new HadoopSpark();
    hs.setId(row.getInt(1));
    hs.setName(row.getString(2));
    hs.setEmail(row.getString(3) );
    hs.setQq(row.getString(4));
    return hs;
  }

}
