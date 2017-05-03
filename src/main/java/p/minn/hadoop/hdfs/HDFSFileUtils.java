package p.minn.hadoop.hdfs;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import p.minn.hadoop.db.Json2dbFileInputFormat;
import p.minn.hadoop.db.Json2dbMapper;
import p.minn.hadoop.db.Json2dbReducer;
import p.minn.hadoop.entity.HadoopSpark;

/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
public class HDFSFileUtils {

	private  String output="output";
	
	private  String input="input";
	
	private String DRIVER_CLASS;
	
	private String DB_URL;
	
	private String username;
	
	private String password;
	
	private Configuration conf;
	
	private  FileSystem hdfs;
	

	public HDFSFileUtils(String defaultFS) throws IOException{
	      System.setProperty("HADOOP_USER_NAME", "minn");
		  conf=new Configuration(); 
		  conf.set("fs.defaultFS", defaultFS);
		  conf.addResource("etc/hadoop/core-site.xml");
		  conf.addResource("etc/hadoop/hdfs-site.xml");
		  hdfs=FTPFileSystem.get(conf);
	}


	public String getOutput() {
		return output;
	}


	public void setOutput(String output) {
		this.output = output;
	}


	public String getInput() {
		return input;
	}


	public void setInput(String input) {
		this.input = input;
	}
	
	

  public void setDriverClass(String driverClass) {
    this.DRIVER_CLASS = driverClass;
  }

  public void setDbUrl(String dbUrl) {
    DB_URL = dbUrl;
  }

  

  public void setUsername(String username) {
    this.username = username;
  }


  public void setPassword(String password) {
    this.password = password;
  }


  public void deleteFile(String filename) throws Exception{
		  Path dst =new Path(input+"/"+filename);  
		  hdfs.deleteOnExit(dst);
	}
	
	public  void uploadFile(String filename,File src){  
        Path dst =new Path(input+"/"+filename);  
        try {  
        	  hdfs.delete(dst, false);
        	  FileUtil.copy(src, hdfs, dst, true, conf);
              src.delete();
        } catch (IOException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
        }  
          
    }  
	
	public List<Map<String,Object>> readFiles() throws Exception{
		  List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
           RemoteIterator<LocatedFileStatus> fs= hdfs.listLocatedStatus(new Path(input));
           int idx=1;
           while(fs.hasNext()){
           	LocatedFileStatus f=fs.next();
           	Map<String,Object> m=new HashMap<String,Object>();
           	m.put("id", idx);
           	m.put("name",f.getPath().getName() );
           	list.add(m);
           	idx++;
           }
           return list;
	}
	
	public void import2db(String fileName) throws Exception{
	   DBConfiguration.configureDB(conf, DRIVER_CLASS, DB_URL,username,password);
	   Job job = Job.getInstance(conf, "json import2db");
	   job.setJar("/usr/local/spark/examples/hadoopspark.jar");
	    job.setMapperClass(Json2dbMapper.class);
	    job.setInputFormatClass(Json2dbFileInputFormat.class);
	    job.setOutputFormatClass(DBOutputFormat.class);
	    job.setReducerClass(Json2dbReducer.class);
	    
	    job.setMapOutputKeyClass(BytesWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(HadoopSpark.class);
	    job.setOutputValueClass(NullWritable.class);

	    Json2dbFileInputFormat.addInputPath(job, new Path(input+fileName));
	    String[] fields = {"name","email","qq"};
	    DBOutputFormat.setOutput(job, "hadoopspark", fields);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


  public String readFileContent(String fileName) throws Exception {
    // TODO Auto-generated method stub
     byte[] data=readFileData(fileName);
    return new String(data);
  }
  public byte[] readFileData(String fileName) throws Exception {
    // TODO Auto-generated method stub
    DataInputStream in=readDataInputStream(fileName);
     byte[] data=new byte[in.available()];
    IOUtils.readFully(in, data, 0, in.available());
    return data;
  }
  
  public DataInputStream readDataInputStream(String fileName) throws Exception {
    // TODO Auto-generated method stub
    Path path=new Path(input+fileName);
    FSDataInputStream in=hdfs.open(path);
    return in;
  }
	
	
}
