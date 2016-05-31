package p.minn.hadoop.db;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
public class Json2dbMapper extends Mapper<Text, BytesWritable, BytesWritable, Text> {

  public void map(Text key, BytesWritable value, Context context) throws IOException,
      InterruptedException {
    context.write(value, key);
  }
}
