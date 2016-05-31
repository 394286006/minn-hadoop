package p.minn.hadoop.repository;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import p.minn.common.utils.Page;

/**
 * 
 * @author minn
 * @QQ:3942986006
 * @omment 
 */
//@LogAnnotation(resourceKey="A000A000")
public interface HadoopSparkDao {
	
public  int getTotal(@Param("lang")  String lang,@Param("condition")  Map<String,String> condition);
	
	public  List<Map<String, Object>> query(@Param("lang")  String lang, @Param("page") Page page,
			@Param("condition") Map<String,String> condition);

}