package nationalityMR_dc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NationalityCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);    
    private static Map<String, Integer> countries = new HashMap<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	URI[] files = context.getCacheFiles();
    	Path cachePath = new Path(files[0]);   	    	
//    	Path[] cachePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	
    	try(BufferedReader reader = new BufferedReader(new FileReader(cachePath.getName()))) {
    		String line = "";
    		while ((line = reader.readLine()) != null) {
    			String words[] = line.split(",");
    			countries.put(words[0], Integer.valueOf(words[1]));
    		}
    	}	    	  
    }
    
    @Override
    public void map(Object key, Text value, Context output) throws IOException, InterruptedException {
    	if(value.toString().equals("name,club,age,position,position_cat,market_value,page_views,fpl_value,fpl_sel,fpl_points,"
    			+ "region,nationality,new_foreign,age_cat,club_id,big_club,new_signing")) return;       
    	String[] words = value.toString().split(",");   	
    	String country = words[11];
        if (!countries.containsKey(country)) return;        
		output.write(new Text(String.valueOf(countries.get(country))), one);
	}
}
