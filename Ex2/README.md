
题目2：编写MapReduce，统计`/user/hadoop/mapred_dev/ip_time` 中去重后的IP数，越节省性能越好。（35分）
结果:1218
//可以在map部分进行hashset去重，可以减少中间的部分。
---

运行完之后，描述程序里所做的优化点，每点+5分。
package wc;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import No1.Haha;

public class Haha1 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			    String a = "null";
				String line = value.toString();
				String str[]=line.split("\t");
				String ip=str[0];
				//key相同的时候在reduce中会被分到一个set里
				context.write(new Text(a),new Text(ip));				
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
			String a = Integer.toString(set.size());
			context.write(new Text(a),new Text(""));
		}
		
	}
	


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Haha1.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
