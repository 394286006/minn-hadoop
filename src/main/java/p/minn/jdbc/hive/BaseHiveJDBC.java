package p.minn.jdbc.hive;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.springframework.jdbc.datasource.DriverManagerDataSource;


/**
 * 
 * @author minn
 * @QQ:3942986006
 * @omment 
 */
public class BaseHiveJDBC<T> implements Serializable{

  private DriverManagerDataSource hivedataSource;

  
  public Connection getConnect() throws Exception{
    return hivedataSource.getConnection();
  }
  
  public  int getTotal(Connection conn,String sqltxt) throws SQLException{
    int count=0;
    Statement stat=conn.createStatement();
    ResultSet rs=stat.executeQuery(sqltxt);
    rs.next();
    count=rs.getInt(1);
    close(stat,rs);
    return count;
  }
  
  public  List<T> pageSql(Connection conn,Function<ResultSet,T> fun,String sqltxt) throws Exception{
    
    List<T> list=new ArrayList<T>();
    Statement stat=conn.createStatement();
    ResultSet rs=stat.executeQuery(sqltxt);
    while(rs.next())  
    {
      list.add(fun.call(rs));
    }
  
    close(stat,rs);
    
    return list;
  }
  
  public void close(Statement stat,ResultSet rs) throws SQLException{
    if(rs!=null){
      rs.close();
    }
    if(stat!=null){
      stat.close();
    }
  }
  
  public void close(Connection conn) throws SQLException{
    if(conn!=null){
      conn.close();
    }
  }
  
  
  public void setHivedataSource(DriverManagerDataSource hivedataSource) {
    this.hivedataSource = hivedataSource;
  }
  
}
