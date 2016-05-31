package p.minn.hadoop.entity;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import p.minn.privilege.entity.IdEntity;

/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
public class HadoopSpark extends IdEntity implements DBWritable {

  String name;

  String email;

  String qq;

  public HadoopSpark() {
    super();
  }

  public HadoopSpark(String name, String email, String qq) {
    super();
    this.name = name;
    this.email = email;
    this.qq = qq;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getQq() {
    return qq;
  }

  public void setQq(String qq) {
    this.qq = qq;
  }

  @Override
  public void readFields(ResultSet rs) throws SQLException {
    // TODO Auto-generated method stub
    this.name = rs.getString(1);
    this.email = rs.getString(2);
    this.qq = rs.getString(3);
  }

  @Override
  public void write(PreparedStatement stat) throws SQLException {
    // TODO Auto-generated method stub
    stat.setString(1, name);
    stat.setString(2, email);
    stat.setString(3, qq);
  }



}
