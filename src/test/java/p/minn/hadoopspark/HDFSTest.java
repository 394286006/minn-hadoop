package p.minn.hadoopspark;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * 
 * @author minn
 * @QQ:3942986006
 *
 */
public class HDFSTest {

	static Configuration conf=new Configuration(); 
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			conf.addResource("etc/hadoop/core-site.xml");
			conf.addResource("etc/hadoop/hdfs-site.xml");
			//conf.set("fs.defaultFS", "hdfs://namenode1:9000");
			 readHDFSListAll();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
	}
	
	 public static void readHDFSListAll() throws Exception{  
         
	     //流读入和写入  
	            InputStream in=null;  
	            //获取HDFS的conf  
	          //读取HDFS上的文件系统  
	           // FileSystem hdfs = FileSystem.get(new URI("hdfs://172.16.232.132:9000"),conf);
	           FileSystem hdfs=FileSystem.get(conf);
	          // System.out.println("hsfs:"+hdfs.getName());
	          //使用缓冲流，进行按行读取的功能  
	            BufferedReader buff=null;  
	          //获取日志文件的根目录  
	            Path listf =new Path("output/");  
	          //获取根目录下的所有2级子文件目录  
	            FileStatus stats[]=hdfs.listStatus(listf);  
	            RemoteIterator<LocatedFileStatus> fs= hdfs.listLocatedStatus(listf);
	            while(fs.hasNext()){
	            	LocatedFileStatus f=fs.next();
	            	System.out.println("file:"+f.getPath().getName());
	            }
	          //自定义j，方便查看插入信息  
	            int j=0;  
	             for(int i = 0; i < stats.length; i++){  
	                //获取子目录下的文件路径  
	                FileStatus   temp[]=hdfs.listStatus(new Path(stats[i].getPath().toString()));  
	                  for(int k = 0; k < temp.length;k++){  
	                      System.out.println("文件路径名:"+temp[k].getPath().toString());  
	                //获取Path  
	                Path p=new Path(temp[k].getPath().toString());  
	                //打开文件流  
	                 in=hdfs.open(p);  
	                 //BufferedReader包装一个流  
	                   buff=new BufferedReader(new InputStreamReader(in));             
	                 String str=null;  
	                 while((str=buff.readLine())!=null){  
	                       
	                     System.out.println(str);  
	                 }  
	                    buff.close();  
	                    in.close();  
	                   
	   
	                 }  
	                   
	                  
	                   
	   
	                  }  
	                   
	             hdfs.close();  
	           
	  
	        }  

}
