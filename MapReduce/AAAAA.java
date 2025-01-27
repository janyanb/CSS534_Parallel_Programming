import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class AAAAA {

public static class KeyWordMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text keyword = new Text();
    private Set<String> keywords;

    public void configure(JobConf job) {
        int argc = Integer.parseInt(job.get("argc"));  //retrieves the number of keywords 
        keywords = new HashSet<>();
        for (int i = 0; i < argc; i++){
            keywords.add(job.get("keyword"+ i));
        }
    }
    
    public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {        

        //get current filename
        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String filename = fileSplit.getPath().getName();

        String [] tokens = value.toString().split("\\s+"); //faster tokenizing with String.split

        //counts occurrences of each keyword
        Map<String,Integer> keywordCounts = new HashMap<>();
        for(String token: tokens){
            if(keywords.contains(token)){
                keywordCounts.put(token, keywordCounts.getOrDefault(token, 0) + 1);
            }
        }

        // Emit each keyword and its filename with the count
        for (Map.Entry<String, Integer> entry : keywordCounts.entrySet()) {
            keyword.set(entry.getKey());
            output.collect(keyword, new Text(filename + " " + entry.getValue()));
        }
   }
}


public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    
    
    Map<String, Integer> DocCounts = new HashMap<>();

    //processes each (filename count) associated with the current key
    while(values.hasNext()) {
        String[] args = values.next().toString().split(" ");
        String filename = args[0];
        int keywordCount = Integer.parseInt(args[1]);

        DocCounts.put(filename, DocCounts.getOrDefault(filename, 0) + keywordCount);
    }

   // Sort the documents in ascending order based on the keyword count
    List<Map.Entry<String, Integer>> sortedDocCounts = new ArrayList<>(DocCounts.entrySet());
    sortedDocCounts.sort((e1, e2) -> Integer.compare(e1.getValue(), e2.getValue()));

    //Build an output string with filename count entries
    StringBuilder docListText = new StringBuilder();
    for(Map.Entry<String, Integer> entry: sortedDocCounts){
        docListText.append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");
    }

    // finally, print it out.
    output.collect(key, new Text(docListText.toString().trim()));
    }
}

public static void main(String[] args) throws Exception{

    if (args.length < 3) {
    System.err.println("Usage: AAAAA <input path> <output path> <keyword1> <keyword2> ...");
    System.exit(-1);
    }
    //get time to measure job execution time 
    long time = System.currentTimeMillis();
    
    JobConf conf = new JobConf(AAAAA.class);
    conf.setJobName("BBBBB");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(KeyWordMap.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.set("argc", String.valueOf(args.length -2));  //Pass the number of keywords(beyond the first 2) to JobConf

    for(int i = 0; i < args.length -2; i++){
        conf.set("keyword" + i, args[i+2]);           //store each keyword in jobConf as keyword0, keyword1..
    }

    JobClient.runJob(conf);

    System.out.println("Elapsed Time = " + (System.currentTimeMillis() - time) + " ms");
}
    
}