package dataRreHandle;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Formatting {
	
	static String INPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Filter/part-r-00000"; 
	static String OUTPUT_PATH = "hdfs://localhost:9000/output/dataprehandle/D_Filter2";
	
	public static final class FormattingMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		    SimpleDateFormat format2 = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
		    SimpleDateFormat format3 = new SimpleDateFormat("MMMM d,yyyy", Locale.ENGLISH);
			String regex1 = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
		    Pattern pattern1 = Pattern.compile(regex1);
		    String regex2 = "[0-9]{4}/[0-9]{2}/[0-9]{2}";
		    Pattern pattern2 = Pattern.compile(regex2);
		    String regex3 = "[a-zA-Z]+ [0-9]+,[0-9]{4}";
		    Pattern pattern3 = Pattern.compile(regex3);
			
			String line = value.toString();
	        String[] values = line.split("\\|");
	        String reviewDate = values[4];
	        String birthday = values[8];
	        String temperature = values[5];
	        
	        try {
	        	if(pattern1.matcher(reviewDate).matches()) {
		        	
		        } else if(pattern2.matcher(reviewDate).matches()) {
		        	reviewDate = format1.format(format2.parse(reviewDate));
		        } else if(pattern3.matcher(reviewDate).matches()) {
		        	reviewDate = format1.format(format3.parse(reviewDate));
		        }
	        	
	        	if(pattern1.matcher(birthday).matches()) {
		        	
		        } else if(pattern2.matcher(birthday).matches()) {
		        	birthday = format1.format(format2.parse(birthday));
		        } else if(pattern3.matcher(birthday).matches()) {
		        	birthday = format1.format(format3.parse(birthday));
		        }
	        } catch (ParseException e) {
                e.printStackTrace();
            }

	        if(temperature.endsWith("℉")) {
	        	double tem = Double.valueOf(temperature.substring(0, temperature.length()-2));
	        	tem = (tem - 32) / 1.8;
	        	temperature = String.valueOf(tem) + "℃";
	        }
	        
	        line = line.replace(values[4], reviewDate);
	        line = line.replace(values[5], temperature);
	        line = line.replace(values[8], birthday);
	        
	        context.write(new Text(line), new Text(""));
		}
		
	}
	
	public static void main(String[] arg) throws Exception{
		
		Path outputpath=new Path(OUTPUT_PATH);
		Path inputpath=new Path(INPUT_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "formatting");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(Formatting.class);
		job.setMapperClass(FormattingMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
	
}
