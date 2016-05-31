package p.minn.hadoop.db;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import p.minn.hadoop.entity.HadoopSpark;

/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
public class Json2dbReducer extends Reducer<BytesWritable, Text, HadoopSpark, NullWritable> {

  NullWritable n = NullWritable.get();

  @Override
  public void reduce(BytesWritable key, Iterable<Text> values, Context context) throws IOException,
      InterruptedException {
    ObjectMapper mapper = new ObjectMapper();
    List<HadoopSpark> list =
        mapper.readValue(key.getBytes(),
            TypeFactory.defaultInstance().constructCollectionType(List.class, HadoopSpark.class));
    for (HadoopSpark tmp : list) {
      context.write(tmp, n);
    }
  }
}
