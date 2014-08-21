package com.ict.hadoop;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;


public class MsgSplitFileRecordReader implements RecordReader<Text, Text>{
	private static Logger log = Logger.getLogger(MsgSplitFileRecordReader.class);
	private BufferedReader reader;
	private String headerString;
	private int msgNum;
	private String keyName;
	private boolean hasNext = true;
	private long start;
	private long length;
	private FSDataInputStream fsDataInputStream;
	public void close() throws IOException {
		if( reader != null ){
			reader.close();
		}
	}

	public MsgSplitFileRecordReader(JobConf jobConf, FileSplit fileSplit) throws IOException{
		//start = fileSplit.getStart();
		length = fileSplit.getLength();
		final Path path = fileSplit.getPath();
		keyName = path.getName().replaceAll("[^a-zA-Z0-9]", "");;
		final FileSystem fs = path.getFileSystem(jobConf);
		fsDataInputStream = fs.open(path);
		start = fsDataInputStream.getPos();
		
		reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
		msgNum = 0;
		
		String line;
		while( ( line = reader.readLine() )!= null){
			if( line.matches("^\\s*<msg.*$")){
				headerString = line;
				break;
			}
		}
		
		if( headerString == null)
			hasNext = false;
		
	}

	
	public boolean next(Text key, Text value) throws IOException{
		if(hasNext == false)
			return hasNext;
		StringBuffer sb =  new StringBuffer(headerString);
		
		String line;
		log.info("Reading a new message...");
		System.out.print("Reading a new message...");
		while( (line = reader.readLine() )!= null){
			if( line.matches("^\\s*<msg.*$")){
				headerString = line;
				msgNum++;
//				key.set(keyName+" "+msgNum);
				key.set(keyName);
				value.set(sb.toString());
				return true;
			}else{
				sb.append("\n");
				sb.append(line);
			}
		}
		msgNum++;
//		key.set(keyName+" "+msgNum);
		key.set(keyName);
		value.set(sb.toString());
		hasNext = false;
		return true;
	}

	public Text createKey() {
		// TODO Auto-generated method stub
		return new Text();
	}

	public Text createValue() {
		// TODO Auto-generated method stub
		return new Text();
	}

	public long getPos() throws IOException {
		return fsDataInputStream.getPos();
	}

	public float getProgress() throws IOException {
		return Math.min(1.0f, (fsDataInputStream.getPos()-start)/(float)length);
	}
}
