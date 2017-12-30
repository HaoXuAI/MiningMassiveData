package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */

    String[] line = value.toString().split("\"");

    if (line.length > 1) {
      String req = line[1];
      String[] req_arr = req.split(" ");
      if (req_arr.length > 1) {
        String file_name = req_arr[1];
        if (file_name.contains("jpg")) {
          context.getCounter("ImageCounter", "jpg").increment(1);
        } else if (file_name.contains("gif")) {
          context.getCounter("ImageCounter", "gif").increment(1);
        } else {
          context.getCounter("ImageCounter", "other").increment(1);
        }
      }
    }

  }
}