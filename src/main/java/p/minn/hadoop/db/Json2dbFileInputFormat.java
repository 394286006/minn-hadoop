package p.minn.hadoop.db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Json2dbFileInputFormat extends FileInputFormat<Text, BytesWritable> {

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return  new HadoopSparkFileRecordReader();
	}

	private  class HadoopSparkFileRecordReader extends RecordReader<Text, BytesWritable> {

	  private Text key = new Text();
	  private BytesWritable value = new BytesWritable();
	  private boolean processed = false;
	  private FSDataInputStream in = null;
	  private FileSplit fileSplit = null;

	  @Override
	  public void close() throws IOException {
	    // TODO Auto-generated method stub
	    if (in != null)
	      in.close();
	  }

	  @Override
	  public Text getCurrentKey() throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    return key;
	  }

	  @Override
	  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    return value;
	  }

	  @Override
	  public float getProgress() throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    return processed ? fileSplit.getLength() : 0;
	  }

	  @Override
	  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
	      InterruptedException {
	    // TODO Auto-generated method stub
	    fileSplit = (FileSplit) split;
	    Configuration job = context.getConfiguration();
	    Path file = fileSplit.getPath();
	    FileSystem fs = file.getFileSystem(job);
	    in = fs.open(file);
	  }

	  @Override
	  public boolean nextKeyValue() throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    if (!processed) {
	      byte[] content = new byte[(int) fileSplit.getLength()];
	      Path file = fileSplit.getPath();
	      key.set(file.getName());
	      try {
	        IOUtils.readFully(in, content, 0, content.length);
	        value.set(content, 0, content.length);
	      } catch (IOException e) {
	        e.printStackTrace();
	      } finally {
	        IOUtils.closeStream(in);
	      }
	      processed = true;
	      return true;
	    }
	    return false;
	  }

	}
}
