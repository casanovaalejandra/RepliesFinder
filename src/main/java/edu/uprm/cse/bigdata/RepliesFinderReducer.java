package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class RepliesFinderReducer extends  Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // setup a counter
        String replies= null;

        for (Text value : values ){
            replies += " "+value.toString();
        }

        // DEBUG
        context.write(key, new Text(replies));
    }
}
