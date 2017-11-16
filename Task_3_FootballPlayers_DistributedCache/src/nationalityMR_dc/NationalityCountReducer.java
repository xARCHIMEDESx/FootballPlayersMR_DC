package nationalityMR_dc;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NationalityCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value: values){
            count += value.get();
        }
        output.write(key, new IntWritable(count));
    }
}
