package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class RepliesFinderMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //ESta clase lo que deber hacer es leer cada twee
        String tweet = value.toString();
        int count_replies = 0;
        //usando twitter4j se convierte el string jason ( el twitter object) a un Status object
        //y con este puedes seleccionar el texto como un field a leer
        //fuente: https://flanaras.wordpress.com/2016/01/11/twitter4j-status-object-string-json/
        Status status = null;
        Text replied_id = null;
        Text reply_text = null;

        try {
            status = TwitterObjectFactory.createStatus(tweet);
            count_replies = (int)(status.getInReplyToStatusId());
            reply_text = new Text(status.getText());
            replied_id = new Text( Long.toString(status.getInReplyToStatusId()));
            if(count_replies>0){
                System.out.println(count_replies+" " +reply_text);
                context.write(reply_text, replied_id);                                                                                                   //respondio con el field in_reply_to_status_id_str

            }
            System.out.print(count_replies);

        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }
}
