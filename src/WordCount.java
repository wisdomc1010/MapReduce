import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		private String log = new String();
		private String time = new String();
		private String info = new String();
		private String error = new String();
		private String fileName = new String();
		
		private MultipleOutputs<Text,IntWritable> mos;
		
		protected void setup(Context context) throws IOException,InterruptedException{
			mos = new MultipleOutputs<Text,IntWritable>(context);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			log = value.toString();
			boolean timeRs = false;
			boolean infoRs = false;
			boolean errorRs = false;
			//匹配日期，如2017-04-24 15:09:52
		    String time_regEx = "\\d+\\-\\d+\\-\\d+ \\d+\\:\\d+\\:\\d+";
		    // 忽略大小写的写法
		    Pattern time_pattern = Pattern.compile(time_regEx, Pattern.CASE_INSENSITIVE);
		    Matcher time_matcher = time_pattern.matcher(log);
		    // 查找字符串中是否有匹配正则表达式的字符/字符串
		    timeRs = time_matcher.find();
		    if(timeRs){
			    time = time_matcher.group();
		    }else{
		    	//匹配info，如[INFO]-[Thread: Thread-1]-[com.unitech.lingpipe.util.Log.logInfo(Log.java:14)]: files.length1
			    String info_regEx = "\\[(.*?)\\]\\-\\[(.*?)\\]\\-\\[(.*?)\\]:\\s+(.*)";
			    // 忽略大小写的写法
			    Pattern info_pattern = Pattern.compile(info_regEx, Pattern.CASE_INSENSITIVE);
			    Matcher info_matcher = info_pattern.matcher(log);
			    // 查找字符串中是否有匹配正则表达式的字符/字符串
			    infoRs = info_matcher.find();
			    if(infoRs){
				    info = info_matcher.group();
			    }else {
			    	//匹配error，如com.mysql.jdbc.exceptions.jdbc4.CommunicationsException: Communications link failure
				    String error_regEx = "((.*?):\\s+(.*))|(.*)";
				    // 忽略大小写的写法
				    Pattern error_pattern = Pattern.compile(error_regEx, Pattern.CASE_INSENSITIVE);
				    Matcher error_matcher = error_pattern.matcher(log);
				    // 查找字符串中是否有匹配正则表达式的字符/字符串
				    errorRs = error_matcher.find();
				    if(errorRs){
					    error = error_matcher.group();
				    }else{
					    error = "";
				    }
				    if((time != null && !time.isEmpty()) && (info != null && !info.isEmpty())){
				    	String wd=new String();  
				        wd=time+"\t"+info+"\t"+error;
				        //wd="areacode|"+areacode +"|imei|"+ imei +"|responsedata|"+ responsedata +"|requesttime|"+ requesttime +"|requestip|"+ requestip;  
				        time = "";
				        info = "";
				        error = "";
				        word.set(wd);  
				        //查看.出现的位置，如果只有1个.，则第0个文件，另外则根据文件后缀命名文件夹
				        if(fileName.lastIndexOf(".")>fileName.indexOf(".")){
				        	System.out.println(fileName.substring(fileName.lastIndexOf("."),fileName.length()));
				        	mos.write(word,one,"yyxt/"+fileName.substring(fileName.lastIndexOf(".")+1,fileName.length())+"/yyxt");
				        }else{
				        	mos.write(word,one,"yyxt/0/yyxt");
				        }
				        
				    }
			    }
		    }
		}
		
		protected void cleanup(Context context) throws IOException,InterruptedException{
			mos.close();
		}
	}
	

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.6.0");
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println(otherArgs.length);
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");  
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 判断output文件夹是否存在，如果存在则删除  
		Path path = new Path(otherArgs[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
		if (fileSystem.exists(path)) {  
		    fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
		}  
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
