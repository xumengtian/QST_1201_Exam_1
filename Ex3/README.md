题目3：编写MapReduce，统计这两个文件

`/user/hadoop/mapred_dev_double/ip_time`

`/user/hadoop/mapred_dev_double/ip_time_2`

当中，重复出现的IP的数量(40分)
结果：550
---
加分项：

1. 写出程序里面考虑到的优化点，每个优化点+5分。
 使用一个mr即可进行。可在map部分对相同文件中的ip相同数据进行去重。
2. 额外使用Hive实现，+5分。最后mr产生的文件处理时，使用hive建表（只有一个字段:ip），将生成的文件导入该表中，使用sql语句查询总数据的数量即ip数
3. 额外使用HBase实现，+5分。
package wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Haha2 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString(); 
			if (filename.equals("ip_time")) {
				String line = value.toString();
				String str[] = line.split(" ");
				String ip=str[0];
				context.write(new Text(ip), new Text("1++"));
			} else if (filename.equals("ip_time_2")) {
				String line = value.toString();
				String str[] = line.split(" ");
				String ip=str[0];
				context.write(new Text(ip), new Text("1"));
			}				
			}
		}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Set<String> set = new HashSet<String>();
			while(values.iterator().hasNext()){
				String line = values.iterator().next().toString();
				set.add(line);				
			}
			if(set.size()>1){
				context.write(new Text(key),new Text(""));
			}

		}
		
	}
	


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Haha2.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0],args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);

	}

}

