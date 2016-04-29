package com.redditanalysis.job;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.redditanalysis.inputformat.XmlInputFormat;
import com.redditanalysis.mapper.ImportReviewMapper;

public class ImportReviewJob extends Configured implements Tool {
	public static void main(String[] args)
	{
		if(args.length<2)
		{
			System.out.println("Java Usage ImportReviewJob /hdfs/path/to/input /hdfs/path/to/output");
			return;
		}
		try {
			Configuration conf=new Configuration(Boolean.TRUE);
			conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
			conf.set("mapreduce.framework.name", "classic");
			conf.set("mapred.job.tracker", "localhost.localdomain:8021");
			
			int i=ToolRunner.run(conf,new ImportReviewJob(), args);
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("Failed");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		

		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		conf.set(XmlInputFormat.START_TAG_KEY, "<document>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</document>");

		Job job = new Job(conf, this.getClass().getName());
		
		job.setJarByClass(ImportReviewJob.class);
		job.setMapperClass(ImportReviewMapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}

}
