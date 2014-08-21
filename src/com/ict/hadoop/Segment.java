package com.ict.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import ICTCLAS.I3S.AC.ICTCLAS50;

public class Segment
{   
	private static Logger logger = Logger.getLogger(Segment.class);
	//停用词词表 
	private static String absolutePathString ;
	//public static final String stopWordTable = new File("").getAbsolutePath()+"/library/stopwordtable"; 
	public static final String stopWordTable = "stopwordtable"; 
	public static Set<String> stopWordSet;
	private static ICTCLAS50 ictclas = new ICTCLAS50();
	
	public static boolean init(String absolutePath){
		absolutePathString = absolutePath;
		if(!absolutePathString.endsWith("/"))
			absolutePathString += "/";
		
		//初始化分词所用库路径
		try {
			if (ictclas.ICTCLAS_Init(absolutePathString.getBytes("GB2312")) == false)
			{
				logger.error("Error 初始化分词所用库出现错误"); 
				return false;
			}
			
			//设置词性标注集(0 计算所二级标注集，1 计算所一级标注集，2 北大二级标注集，3 北大一级标注集)
			ictclas.ICTCLAS_SetPOSmap(2);

			//导入用户字典
			String usrdir =absolutePathString +"userdict.txt"; //用户字典路径
//			String usrdir = "userdict.txt";
			byte[] usrdirb = usrdir.getBytes();//将string转化为byte类型
			//导入用户字典,返回导入用户词语个数第一个参数为用户字典路径，第二个参数为用户字典的编码类型
			ictclas.ICTCLAS_ImportUserDictFile(usrdirb, 0);

		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage(),e);
			return false;
		}
		
		try{
			BufferedReader StopWordFileBr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(absolutePathString+stopWordTable))));
			//用来存放停用词的集合     
			stopWordSet = new HashSet<String>();
			//初如化停用词集   
			String stopWord = null;
			for(; (stopWord = StopWordFileBr.readLine()) != null;)
			{     
				stopWordSet.add(stopWord);    
			}
		} catch (FileNotFoundException e) {
			logger.warn(e.getMessage(),e);
		} catch (IOException e) {
			logger.warn(e.getMessage(),e);
		}
		
		return true;
	}
	public static String segString(String sInput,int isposmap)
	{
		if(absolutePathString ==  null){
			logger.error("The absolute path is not setted.");
			return null;
		}
		StringBuffer spiltwords= new StringBuffer();
		try
		{		
			String line = "";
			String[] lines = sInput.split("\n");
			
			for(int k=0;k<lines.length;k++){
				line = lines[k];
			
				if(line.startsWith("<msg")||line==null||line.length()==0) {
					spiltwords.append(line);
					continue;
				}
				//对文本信息进行分词
				byte[] spiltResult = ictclas.ICTCLAS_ParagraphProcess(line.getBytes("gb2312"), 2, isposmap);
				String spiltResultStr = new String(spiltResult,0,spiltResult.length,"gb2312");
				//得到分词后的词汇数组，以便后续比较     
				String[] resultArray = spiltResultStr.split(" ");
				
				int count=0;
				for(int i = 0; i<resultArray.length; i++){ 
					//过滤一：空字符or重复字符
					if(resultArray[i].equals("")||resultArray[i].matches("-+")
							||resultArray[i].matches("~+")) {resultArray[i]=null;continue;}
					//过滤二：停用词
					int index = resultArray[i].indexOf("/");
					if(index==-1)index = resultArray[i].length();
					if(stopWordSet.contains(resultArray[i].substring(0,index))){       
						resultArray[i] = null;
						count++;continue;
					}
				}
				//全部为停用词
				spiltwords.append("\n");
				if(count==resultArray.length)continue;
				
				for(int i = 0; i<resultArray.length; i++){      
					if(resultArray[i] != null){     
					      spiltwords.append(resultArray[i]+" ");
					} 
				}
				
				
			}
			spiltwords.append("\n");
			
		}
		catch (Exception ex)
		{
			logger.error(ex.getMessage(),ex);
			return null;
		}
		return spiltwords.toString();
	}
	
	public static void close(){
		//保存用户字典
		ictclas.ICTCLAS_SaveTheUsrDic();
		//释放分词组件资源
		ictclas.ICTCLAS_Exit();
	}
}


	
	

	