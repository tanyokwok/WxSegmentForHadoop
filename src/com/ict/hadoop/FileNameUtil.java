package com.ict.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class FileNameUtil {
	private static Logger logger = Logger.getLogger(FileNameUtil.class);
	
	public static Set<String> listAll(String dir) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(dir));
		Set<String> list = new HashSet<String>();
		for (int i = 0; i < stats.length; ++i)
		{
			if ( !stats[i].isDir() )
			{
				String filename = stats[i].getPath().getName();
				filename = getFormatedName(filename);
				list.add(filename);
				//System.out.println("File: "+ stats[i].getPath().getName());
				logger.info("File: "+ filename);
			}
			else
				logger.warn(stats[i].getPath().getName() +" is a directory.");

		}
		fs.close();
		return list;
	}
	
	
	public static String getFormatedName(String filename){
		filename = filename.replaceAll("[^a-zA-Z0-9]", "");
		int idx = filename.lastIndexOf('r');
		if( idx > 0)
			filename = filename.substring(0,idx);
		return filename;
	}
}
