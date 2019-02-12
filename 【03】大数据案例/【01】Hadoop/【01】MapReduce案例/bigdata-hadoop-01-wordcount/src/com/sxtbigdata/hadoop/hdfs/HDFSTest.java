package com.sxtbigdata.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 测试HDFS Hello World
 * @author LGX
 *
 */
public class HDFSTest {
	
	private Configuration conf = null;
	private FileSystem fs = null;
	
	@Before
	public void conn() throws Exception {
		conf = new Configuration(true);
		fs = FileSystem.get(conf);
	}
	
	@After
	public void close() throws Exception {
		fs.close();
	}
	
	@Test
	public void mkdir() throws Exception {
		Path path = new Path("/data");
		if(fs.exists(path)) {
			fs.delete(path, true);
		}else {			
			fs.mkdirs(path);
		}
	}
	
	@Test
	public void upload() throws Exception {
		InputStream inputStream = new FileInputStream(new File("C:\\Users\\LGX\\Desktop\\TianQi.txt"));
		Path f = new Path("/data/TianQi.txt");
		FSDataOutputStream dataOutputStream = fs.create(f);
		IOUtils.copyBytes(inputStream, dataOutputStream, conf, true);
		System.out.println("upload OK...");
	}
	
	@Test
	public void writeTest() throws Exception {
//		1951-07-03 12:21:03	47c
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\LGX\\Desktop\\TianQi.txt"));
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		for(int i = 0;i < 1000; i++) {
			sb.append(randomDate());
			sb.append("\t");
			sb.append(random.nextInt(50) - 10);
			sb.append("c");
			sb.append("\r\n");
			System.out.println(sb.toString());
			fos.write(sb.toString().getBytes());
			sb.delete(0, sb.length());
		}
		
	}
	
	private String randomDate(){
        Random rndYear=new Random();
        int year=rndYear.nextInt(118)+1900;
        Random rndMonth=new Random();
        int month=rndMonth.nextInt(12)+1;
        Random rndDay=new Random();
        int Day=rndDay.nextInt(28)+1;
        Random rndHour=new Random();
        int hour=rndHour.nextInt(23);
        Random rndMinute=new Random();
        int minute=rndMinute.nextInt(60);
        Random rndSecond=new Random();
        int second=rndSecond.nextInt(60);
      return year+"-"+cp(month)+"-"+cp(Day)+" "+cp(hour)+":"+cp(minute)+":"+cp(second);
    }
	
    private String cp(int num){
        String Num=num+"";
        if (Num.length()==1){
         return "0"+Num;
        }else {
            return Num;
        }
    }
    
}
