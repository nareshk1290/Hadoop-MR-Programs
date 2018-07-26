import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	Text word = new Text();
	IntWritable one = new IntWritable(1);
	IntWritable wordLength = new IntWritable();

	
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		
		String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            wordLength.set(tokenizer.nextToken().length());
            con.write(new Text(wordLength.toString()), one);     
	    }
		
	}

}
