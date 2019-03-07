# MapReduce简单案例1：找出每个月气温最高的2天

# 1. 数据集具体格式如下：

```shell
1951-07-03 12:21:03	47c
1949-10-01 14:21:02	34c
1949-10-01 19:21:02	38c
1949-10-02 14:01:02	36c
1950-01-01 11:21:02	32c
1950-10-01 12:21:02	37c
1951-12-01 12:21:02	23c
1950-10-02 12:21:02	41c
1950-10-03 12:21:02	27c
1951-07-01 12:21:02	45c
1951-07-02 12:21:02	46c
1951-07-03 12:21:03	47c
...
...
```

# 2. 需求：找出每个月气温最高的2天
## 2.1 前置准备：生成气温数据文件（TianQi.txt）并上传到HDFS上/data/目录下

```java
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
	
	//上传文件到HDFS
	@Test
	public void upload() throws Exception {
		InputStream inputStream = new FileInputStream(new File("C:\\Users\\LGX\\Desktop\\TianQi.txt"));
		Path f = new Path("/data/TianQi.txt");
		FSDataOutputStream dataOutputStream = fs.create(f);
		IOUtils.copyBytes(inputStream, dataOutputStream, conf, true);
		System.out.println("upload OK...");
	}
	
	//创建气温数据
	//1951-07-03 12:21:03	47c
	@Test
	public void writeTest() throws Exception {
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\LGX\\Desktop\\TianQi.txt"));
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		for(int i = 0;i < 1000000; i++) {
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

```


# 3. 具体代码
## 3.0 具体目录结构：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190209213212385.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2wxMzM2MDM3Njg2,size_16,color_FFFFFF,t_70)

## 3.1 MyTQ .java
```java
/**
 * tianqi Main
 * @author LGX
 *
 */
public class MyTQ {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		
		Job job = Job.getInstance(conf);
		job.setJobName("analyse-tq");
		job.setJarByClass(MyTQ.class);
		
		//Input Output
		Path inputPath = new Path("/data/TianQi.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		Path outputPath = new Path("/data/analyse/");
		if(outputPath.getFileSystem(conf).exists(outputPath)) {
			outputPath.getFileSystem(conf).delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		//MapTask
		job.setMapperClass(TMapper.class);
		job.setMapOutputKeyClass(TQ.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(TPartitioner.class);
		
		job.setSortComparatorClass(TSortComparator.class);
		
		job.setCombinerClass(TCombiner.class);
		job.setCombinerKeyGroupingComparatorClass(TGroupingComparator.class);
		

		
		//ReduceTask
		job.setGroupingComparatorClass(TGroupingComparator.class);
		job.setReducerClass(TReducer.class);
		
		job.waitForCompletion(true);
		
	}
}

```

## 3.2 TQ .java

```java
/**
 * TianQi Type
 * @author LGX
 *
 */
public class TQ implements WritableComparable<TQ>{
	
	private int year;
	private int month;
	private int day;
	private int wd;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeInt(this.day);
		out.writeInt(this.wd);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();
	}

	@Override
	public int compareTo(TQ that) {
		int c1 = Integer.compare(this.year, that.getYear());
		if(c1 == 0) {
			int c2 = Integer.compare(this.month, that.getMonth());
			if(c2 == 0) {
				return Integer.compare(this.day, that.getDay());
			}
			return c2;
		}
		return c1;
	}

}

```

## 3.3 TMapper .java
```java
public class TMapper extends Mapper<LongWritable, Text, TQ, IntWritable>{
	
	private TQ Tkey = new TQ();
	private IntWritable TValue = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TQ, IntWritable>.Context context)
			throws IOException, InterruptedException {
			//1949-10-01 14:21:02	34c
			try {
				String[] valueStrs = StringUtils.split(value.toString(), '\t');
				
				//analyse date
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date date = sdf.parse(valueStrs[0]);
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				Tkey.setYear(cal.get(Calendar.YEAR));
				Tkey.setMonth(cal.get(Calendar.MONTH) + 1);
				Tkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
				Tkey.setWd(Integer.parseInt(valueStrs[1].substring(0, valueStrs[1].length() - 1)));
				
				//analyse wd
				TValue.set(Integer.parseInt(valueStrs[1].substring(0, valueStrs[1].length() - 1)));
				
				context.write(Tkey, TValue);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
	}

}

```
## 3.4 TPartitioner .java
```java
/**
 * 设置分区器 K V P
 * 用于分配相应的Reduce块
 * @author LGX
 *
 */
public class TPartitioner extends Partitioner<TQ, IntWritable>{

	@Override
	public int getPartition(TQ key, IntWritable value, int numPartitions) {
		return key.getYear() % numPartitions;
	}

}
```

## 3.5 TSortComparator .java
```java
public class TSortComparator extends WritableComparator{

	public TSortComparator() {
		super(TQ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TQ obj1 = (TQ)a;
		TQ obj2 = (TQ)b;

		int c1 = Integer.compare(obj1.getYear(), obj2.getYear());
		if(c1 == 0) {
			int c2 = Integer.compare(obj1.getMonth(), obj2.getMonth());
			if(c2 == 0) {
				return -Integer.compare(obj1.getWd(), obj2.getWd());
			}
			return c2;
		}
		return c1;
	}

}
```
## 3.6 TCombiner .java
```java
public class TCombiner extends Reducer<TQ, IntWritable, TQ, IntWritable>{

	private TQ rkey = new TQ();
	private IntWritable rvalue = new IntWritable();
	
	@Override
	protected void reduce(TQ key, Iterable<IntWritable> values, Reducer<TQ, IntWritable, TQ, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//原语：相同的Key为一组，调用一次Reduce方法
		int flg = 0;
		int day = 0;
		for (IntWritable intWritable : values) {
			if(flg == 0) {
				rkey = key;
				rvalue.set(key.getWd());
				context.write(rkey, rvalue);
				flg++;
				day = key.getDay();
			}
			if(flg != 0 && day != key.getDay()) {
				rkey = key;
				rvalue.set(key.getWd());
				context.write(rkey, rvalue);
				break;
			}
		}	
	}
}
```

## 3.7 TGroupingComparator .java
```java
public class TGroupingComparator extends WritableComparator{

	public TGroupingComparator() {
		super(TQ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TQ obj1 = (TQ)a;
		TQ obj2 = (TQ)b;

		int c1 = Integer.compare(obj1.getYear(), obj2.getYear());
		if(c1 == 0) {
			return Integer.compare(obj1.getMonth(), obj2.getMonth());
		}
		return c1;
	}
}
```
## 3.8 TReducer .java
```java
public class TReducer extends Reducer<TQ, IntWritable, Text, IntWritable>{
	
	private Text rkey = new Text();
	private IntWritable rvalue = new IntWritable();

	@Override
	protected void reduce(TQ key, Iterable<IntWritable> values, Reducer<TQ, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//原语：相同的Key为一组，调用一次Reduce方法
		int flg = 0;
		int day = 0;
		for (IntWritable intWritable : values) {
			if(flg == 0) {
				rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
				rvalue.set(key.getWd());
				context.write(rkey, rvalue);
				flg++;
				day = key.getDay();
			}
			if(flg != 0 && day != key.getDay()) {
				rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
				rvalue.set(key.getWd());
				context.write(rkey, rvalue);
				break;
			}
		}
	}
}
```

# 4. 导出Jar包

## 4.1 将当前项目导出成一个jar包上传到Hadoop集群上
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190209213848255.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2wxMzM2MDM3Njg2,size_16,color_FFFFFF,t_70)
## 4.2 使用xftp上传到集群上
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190209213928257.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2wxMzM2MDM3Njg2,size_16,color_FFFFFF,t_70)

# 5. 执行
运行命令
`hadoop jar  jar包名  完整类名`

```
hadoop jar MyTQ.jar com.hadoop.mr.MyTQ
```
可以看到：

```shell
[root@node3 ~]# hadoop jar MyTQ.jar com.hadoop.mr.MyTQ
19/02/04 07:26:44 INFO client.ConfiguredRMFailoverProxyProvider: Failing over to rm2
19/02/04 07:26:44 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
19/02/04 07:26:45 INFO input.FileInputFormat: Total input paths to process : 1
19/02/04 07:26:45 INFO mapreduce.JobSubmitter: number of splits:1
19/02/04 07:26:45 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1549208398103_0008
19/02/04 07:26:45 INFO impl.YarnClientImpl: Submitted application application_1549208398103_0008
19/02/04 07:26:45 INFO mapreduce.Job: The url to track the job: http://node4:8088/proxy/application_1549208398103_0008/
19/02/04 07:26:45 INFO mapreduce.Job: Running job: job_1549208398103_0008
19/02/04 07:26:56 INFO mapreduce.Job: Job job_1549208398103_0008 running in uber mode : false
19/02/04 07:26:56 INFO mapreduce.Job:  map 0% reduce 0%
19/02/04 07:27:09 INFO mapreduce.Job:  map 57% reduce 0%
19/02/04 07:27:12 INFO mapreduce.Job:  map 67% reduce 0%
19/02/04 07:27:14 INFO mapreduce.Job:  map 100% reduce 0%
19/02/04 07:27:22 INFO mapreduce.Job:  map 100% reduce 100%
19/02/04 07:27:22 INFO mapreduce.Job: Job job_1549208398103_0008 completed successfully
19/02/04 07:27:22 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=62310
		FILE: Number of bytes written=344919
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=24820451
		HDFS: Number of bytes written=36620
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=15905
		Total time spent by all reduces in occupied slots (ms)=4698
		Total time spent by all map tasks (ms)=15905
		Total time spent by all reduce tasks (ms)=4698
		Total vcore-milliseconds taken by all map tasks=15905
		Total vcore-milliseconds taken by all reduce tasks=4698
		Total megabyte-milliseconds taken by all map tasks=16286720
		Total megabyte-milliseconds taken by all reduce tasks=4810752
	Map-Reduce Framework
		Map input records=1000000
		Map output records=1000000
		Map output bytes=20000000
		Map output materialized bytes=62310
		Input split bytes=97
		Combine input records=1000000
		Combine output records=2832
		Reduce input groups=1416
		Reduce shuffle bytes=62310
		Reduce input records=2832
		Reduce output records=2832
		Spilled Records=5664
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=793
		CPU time spent (ms)=11390
		Physical memory (bytes) snapshot=299294720
		Virtual memory (bytes) snapshot=4133277696
		Total committed heap usage (bytes)=137498624
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=24820354
	File Output Format Counters 
		Bytes Written=36620
You have new mail in /var/spool/mail/root

```

![在这里插入图片描述](https://img-blog.csdnimg.cn/20190209214247153.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2wxMzM2MDM3Njg2,size_16,color_FFFFFF,t_70)
运行结束后查看结果：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190209214327758.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2wxMzM2MDM3Njg2,size_16,color_FFFFFF,t_70)
使用命令`hadoop dfs -get /data/analyse/* ./`下载相应结果文件，命令依照自己情况更改`hadoop dfs -get 需要下载的文件 下载的本地目录`

```shell
[root@node3 MyTQ]# clear
[root@node3 MyTQ]# hadoop dfs -get /data/analyse/* ./
DEPRECATED: Use of this script to execute hdfs command is deprecated.
Instead use the hdfs command for it.

[root@node3 MyTQ]# ls
part-r-00000  _SUCCESS
[root@node3 MyTQ]# 

```
使用`cat part-r-00000`命令查看结果：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190209214639517.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2wxMzM2MDM3Njg2,size_16,color_FFFFFF,t_70)