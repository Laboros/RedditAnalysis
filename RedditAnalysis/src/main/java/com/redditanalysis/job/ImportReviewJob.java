package com.redditanalysis.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
			int i=ToolRunner.run(new ImportReviewJob(), args);
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
		
		Configuration conf=super.getConf();
		conf.set("mapreduce.framework.name", "local");
		Job importReviewJob=Job.getInstance(conf, this.getClass().getName());
		conf.set(XmlInputFormat.START_TAG_KEY, "<document>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</document>");
		
		importReviewJob.setNumReduceTasks(0);
		importReviewJob.setMapperClass(ImportReviewMapper.class);

		importReviewJob.setOutputKeyClass(Text.class);
		importReviewJob.setOutputValueClass(NullWritable.class);

		importReviewJob.setInputFormatClass(XmlInputFormat.class);
		importReviewJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(importReviewJob, new Path(args[0]));
		TextOutputFormat.setOutputPath(importReviewJob, new Path(args[1]));

		importReviewJob.waitForCompletion(Boolean.TRUE);

		return 0;
	}

}
