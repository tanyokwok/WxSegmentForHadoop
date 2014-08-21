package com.ict.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


public class WxSegment{
	private static Logger logger = Logger.getLogger(WxSegment.class);
	
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text>
	{
		
//	
		@Override
		public void configure(JobConf jobConf){
			super.configure(jobConf);
			if(Segment.init(new File("").getAbsolutePath()+"/ICTClassLib/") == false)
			{
				logger.error("ICTClass初始化错误,系统退出");
				System.exit(1);
			}
		}
		public String getSplitFileName(InputSplit inputSplit){
			return ((FileSplit)inputSplit).getPath().getName();
		}
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text,Text> output, Reporter reporter)
				throws IOException {
//			logger.info("AbsolutePath:" + new File("./ICTClassLib").getAbsolutePath());
//			try {
//				Thread.sleep(100000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			
			String splitFileName = getSplitFileName(reporter.getInputSplit());
			splitFileName = splitFileName.replaceAll("[^a-zA-Z0-9]", "");
//			logger.info(splitFileName);
//			System.out.println(splitFileName);
			String lineString = value.toString();
			
			lineString = Segment.segString(lineString, 1);
			if(lineString != null){
				output.collect(key, new Text(lineString));
			}
//			logger.info(lineString);
//			
			
		}
		@Override
		public void close() throws IOException {
			Segment.close();
        }

	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, NullWritable>{
		private MultipleOutputs mos;
		private OutputCollector<Text,NullWritable> collector;
		
		@Override
		public void configure(JobConf jobConf){
			super.configure(jobConf);
			mos = new MultipleOutputs(jobConf);
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			collector = mos.getCollector(key.toString(), reporter);
			int count = 0;
			while( values.hasNext() ){
				Text temp = values.next();
				collector.collect(new Text(temp),NullWritable.get());
				count ++;
			}
			output.collect(new Text("File "+key.toString()+" has "+ count +" message(s)."), NullWritable.get());
		}
		@Override
		public void close() throws IOException {
            mos.close();
        }
	}
	
	
	public static void main(String [] args) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        //	new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (remainingArgs.length != 2) {
            System.err.println("Error!");
            System.exit(1);
        }
        
       
        
		JobConf jobConf = new JobConf(conf,WxSegment.class);
		
		
		DistributedCache.createSymlink(jobConf);
		logger.info("AbsolutePath:" + new File("").getAbsolutePath());

		jobConf.set("java.library.path","./ICTClassLib");
	    URI uri = null;
		try {
			uri = new URI("hdfs://yq-fx-svr1:54310/user/daehbase/ICTClassLib.jar#ICTClassLib");
		} catch (URISyntaxException e) {
			logger.error(e.getMessage(),e);
		}
	    if( uri != null)
	    	logger.info(uri.getPath());
	    DistributedCache.addCacheArchive(uri, jobConf);
	    
	    
		
		Path in = new Path(remainingArgs[0]);
        Path out = new Path(remainingArgs[1]);
        FileInputFormat.setInputPaths(jobConf, in);
        FileOutputFormat.setOutputPath(jobConf, out);
        
        jobConf.setJobName("WxSegment");
        jobConf.setMapperClass(Map.class);
        jobConf.setReducerClass(Reduce.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setInputFormat(MsgSplitInputFormat.class);
        
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(NullWritable.class);
        
//        jobConf.setNumReduceTasks(0);
        Set<String> set = FileNameUtil.listAll(remainingArgs[0]);
        for( String filename : set){
        	MultipleOutputs.addNamedOutput(jobConf,
                    filename,
                    TextOutputFormat.class,
                    Text.class,
                    NullWritable.class);
        }
 
       jobConf.setJarByClass(WxSegment.class);
        
        JobClient.runJob(jobConf);
	}
}