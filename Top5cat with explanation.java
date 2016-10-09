/*
 @author Gaurav Dangi
*/

//top 5 categories of videos in youtube

//CONFIGURATION CODE

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// MAPPER CODE

   public class Top5_categories {  //we are taking a class by name Top5_categories

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> { //we are extending the Mapper default class having the arguments keyIn as LongWritable and ValueIn as Text and KeyOut as Text and ValueOut as IntWritable

       private Text category = new Text();  //we are declaring a private Text variable "category" which will store the category of videos in youtube
       private final static IntWritable one = new IntWritable(1);  //we are declaring a private final static IntWritable variable "one" which will be constant for every value. MapReduce deals with Key and Value pairs. 
       public void map(LongWritable key, Text value, Context context ) //we are overriding the map method which will run one time for every line
       throws IOException, InterruptedException { 
           String line = value.toString(); //we are storing the line in a string variable line
           String str[]=line.split("\t"); //we are splitting the line by using tab "\t" delimiter and storing the values in a String Array so that all the columns in a row are stored in the string array.

          if(str.length > 5){   //we are taking a condition if we have the string array of length greater than 5 which means if the line or row has at least 6 columns then it will enter into the if condition and execute the code to eliminate the ArrayIndexOutOfBoundsException.
                category.set(str[3]); //we are storing the category which is in 3rd column
          }

      context.write(category, one); //we are writing the key and value into the context which will be the output of the map method
      }

    }

//REDUCER CODE
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> { //extends the default Reducer class with arguments KeyIn as Text and ValueIn as IntWritable which are same as the outputs of the mapper class and KeyOut as Text and ValueOut as IntWritbale which will be final outputs of our MapReduce program.

       public void reduce(Text key, Iterable<IntWritable> values, Context context) //we are overriding the Reduce method which will run each time for every key
         throws IOException, InterruptedException { 
           int sum = 0;                                              //we are declaring an integer variable sum which will store the sum of all the values for each key.
           for (IntWritable val : values) {   //a foreach loop is taken which will run each time for the values inside the "Iterable values" which are coming from the shuffle and sort phase after the mapper phase.

               sum += val.get();    //we are storing and calculating the sum of the values.
           }
           context.write(key, new IntWritable(sum));  //writes the respected key and the obtained sum as value to the context.
       }
    }

//MAIN METHOD
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

      @SuppressWarnings("deprecation")
       Job job = new Job(conf, "categories");
       job.setJarByClass(Top5_categories.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(IntWritable.class);
      //job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);

       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
       job.waitForCompletion(true);
    }

  }
