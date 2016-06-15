package p.minn.spark.map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import p.minn.hadoop.entity.HadoopSpark;
/**
 * 
 * @author minn
 * @QQ:3942986006
 * @omment 
 */
public class MapHadoopSparkFun implements Function<Row,HadoopSpark> {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  public HadoopSpark call(Row row) throws Exception {
    // TODO Auto-generated method stub
    HadoopSpark hs=new HadoopSpark();
    hs.setId(row.getInt(0));
    hs.setName(row.getString(1));
    hs.setEmail(row.getString(2) );
    hs.setQq(row.getString(3));
    return hs;
  }

}
