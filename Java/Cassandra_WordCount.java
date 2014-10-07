import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/*
/bin/rm -rf WordCount_classes; 
mkdir WordCount_classes
classpath=. && for jar in /usr/share/brisk/{cassandra,hadoop}/lib/*.jar;
do classpath=$classpath:$jar done
javac -classpath $classpath -d WordCount_classes WordCount.java
jar -cvf /root/brisk/WordCount.jar -C WordCount_classes/ .
brisk hadoop jar /root/brisk/WordCount.jar WordCount 
[default@wordcount] get output[ascii('bible')]['advantage'];
=> (column=advantage, value=15, timestamp=1308734369159)
*/

/**
 * Counts the occurrences of words in ColumnFamily "input", that have a single column containing a sequence of words.
 *
 * For each word, we output the total number of occurrences.
 *
 * When outputting to Cassandra, we write the word counts as a {word, count} key/value pair,
 * with a row key equal to the name of the source column we read the words from.
 */
public class WordCount extends Configured implements Tool {

    public static class MapperClass extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;
        
        String punctuations[] = { "\"", "'", ",", ";", "!", ":", "\\?", "\\.", "\\(", "\\-", "\\[", "\\)", "\\]" };

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            sourceColumn = ByteBufferUtil.bytes(context.getConfiguration().get("columnname"));         
        }

        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, 
                        InterruptedException {   
                              
            // Fetch columns
            IColumn column = columns.get(sourceColumn);

            if (column == null)
                return;

            String value = ByteBufferUtil.string(column.value());
            
            value = value.toLowerCase();

            // We replace punctuations with empty string
            for (String pattern : punctuations) {
              value = value.replaceAll(pattern, "");
            }            

            StringTokenizer itr = new StringTokenizer(value);

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>> {
        private ByteBuffer outputKey;

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            outputKey = ByteBufferUtil.bytes(context.getConfiguration().get("columnname"));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, 
                           InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
        }

        // See Cassandra API (http://wiki.apache.org/cassandra/API)
        private static Mutation getMutation(Text word, int sum) {
            Column c = new Column();
            c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
            c.setValue(ByteBufferUtil.bytes(String.valueOf(sum)));
            c.setTimestamp(System.currentTimeMillis());

            Mutation m = new Mutation();
            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
            m.column_or_supercolumn.setColumn(c);

            return m;
        }
    }

    public int run(String[] args) throws Exception {
        String columnName = "books";
        getConf().set("columnname", columnName);

        Job job = new Job(getConf(), "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);

        // Tell the Mapper to expect Cassandra columns as input
        job.setInputFormatClass(ColumnFamilyInputFormat.class); 

        // Tell the "Shuffle/Sort" phase what type of key/value pair to expect from Mapper        
        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputFormatClass(ColumnFamilyOutputFormat.class);        
        job.setOutputKeyClass(ByteBuffer.class);
        job.setOutputValueClass(List.class);

        // Set keyspace and column family for the output
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "wordcount", "output");
        // Set keyspace and column family for the input
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), "wordcount", "input");
        
        ConfigHelper.setRpcPort(job.getConfiguration(), "RPC_PORT");
        ConfigHelper.setInitialAddress(job.getConfiguration(), "IP_ADDRESS");
        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
      
        // Set the predicate that determines what columns will be selected from each row
        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));

        // Each row will be handled by one Map job
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0:1;
    }
    
    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle command-line options
        ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(0);
    }    
}
