package com.ict.hadoop;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class MsgSplitInputFormat extends FileInputFormat{

	private static Logger log = Logger.getLogger(MsgSplitInputFormat.class);

	
	@Override
	public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf,
			Reporter reporter) throws IOException {
		System.out.println("Get RecordReader...");
		log.info("Get RecordReader...");
		reporter.setStatus(inputSplit.toString());
		return new MsgSplitFileRecordReader(jobConf,(FileSplit)inputSplit);
	}

}
