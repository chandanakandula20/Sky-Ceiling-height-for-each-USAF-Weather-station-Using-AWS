import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Text;

public class SkyCeilingMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 99999;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String month = line.substring(19, 21);
    int height;
    height = Integer.parseInt(line.substring(70, 75));
    
    String quality = line.substring(75, 76);
    if (height != MISSING && quality.matches("[01459]")) {
      context.write(new Text(month), new IntWritable(height));
    }
  }
}
// ^^ MaxTemperatureMapper
