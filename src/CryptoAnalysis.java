import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;

public class CryptoAnalysis {

    // Volatility Mapper
    public static class VolatilityMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text symbol = new Text();
        private DoubleWritable volatility = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 8) { // Ensure there are enough columns
                try {
                    double high = Double.parseDouble(tokens[1]);
                    double low = Double.parseDouble(tokens[2]);
                    symbol.set(tokens[8]);  // Set symbol from the last column (index 8)
                    volatility.set(high - low);
                    context.write(symbol, volatility);
                } catch (NumberFormatException e) {
                    // Skip invalid rows
                }
            }
        }
    }
    
    // Volatility Reducer
    public static class VolatilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private PriorityQueue<SymbolValuePair> topVolatility;

    @Override
    protected void setup(Context context) {
        // Min-heap: poll() evicts the smallest, so only the 10 largest volatilities survive
        topVolatility = new PriorityQueue<>(10, (a, b) -> Double.compare(a.getValue(), b.getValue()));
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double totalVolatility = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            totalVolatility += val.get();
            count++;
        }
        double avgVolatility = totalVolatility / count; // average volatility

        // Add the symbol and its volatility to the priority queue
        topVolatility.add(new SymbolValuePair(key.toString(), avgVolatility));

        if (topVolatility.size() > 10) {
            topVolatility.poll(); // evict the current minimum, keeping the 10 largest
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Convert the priority queue to a sorted list and emit top 10 symbols with the highest volatility
        List<SymbolValuePair> sortedList = new ArrayList<>(topVolatility);
        sortedList.sort((a, b) -> Double.compare(b.getValue(), a.getValue())); // Sort descending order

        for (SymbolValuePair pair : sortedList) {
            result.set(pair.getValue());
            context.write(new Text(pair.getSymbol()), result);
        }
    }
}


    // Performance Mapper
    public static class PerformanceMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text symbol = new Text();
        private DoubleWritable performance = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 8) { // Ensure there are enough columns
                try {
                    double open = Double.parseDouble(tokens[0]);
                    double close = Double.parseDouble(tokens[3]);
                    symbol.set(tokens[8]); // Set symbol from the last column (index 8)
                    performance.set((close - open) / open * 100); // Percentage change
                    context.write(symbol, performance);
                } catch (NumberFormatException e) {
                    // Skip invalid rows
                }
            }
        }
    }
// Performance Reducer — surfaces the 10 worst-performing symbols (most negative avg % change)
public static class PerformanceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    private PriorityQueue<SymbolValuePair> worstPerformers;

    @Override
    protected void setup(Context context) {
        // Max-heap: poll() evicts the largest (least negative), keeping the 10 most negative values
        worstPerformers = new PriorityQueue<>(10, (a, b) -> Double.compare(b.getValue(), a.getValue()));
    }

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double totalPerformance = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            totalPerformance += val.get();
            count++;
        }
        double avgPerformance = totalPerformance / count;

        worstPerformers.add(new SymbolValuePair(key.toString(), avgPerformance));

        if (worstPerformers.size() > 10) {
            worstPerformers.poll(); // evict the least-negative entry, keeping the 10 worst
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<SymbolValuePair> sortedList = new ArrayList<>(worstPerformers);
        sortedList.sort((a, b) -> Double.compare(b.getValue(), a.getValue())); // least negative → most negative

        for (SymbolValuePair pair : sortedList) {
            result.set(pair.getValue());
            context.write(new Text(pair.getSymbol()), result);
        }
    }
}

// Helper class
public static class SymbolValuePair {
    private String symbol;
    private double value;

    public SymbolValuePair(String symbol, double value) {
        this.symbol = symbol;
        this.value = value;
    }

    public String getSymbol() {
        return symbol;
    }

    public double getValue() {
        return value;
    }
}

    
// Mapper Class
    public static class CryptoMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text symbol = new Text();
        private Text volumeTimestamp = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length >= 9) { // Ensure sufficient columns exist
                try {
                    String timestamp = tokens[6]; // Timestamp (assume index 6)
                    String cryptoSymbol = tokens[8]; // Symbol (assume index 8)
                    double volume = Double.parseDouble(tokens[4]); // Volume (index 4)

                    // Emit symbol as key and volume + timestamp as value
                    symbol.set(cryptoSymbol);
                    volumeTimestamp.set(volume + "," + timestamp);
                    context.write(symbol, volumeTimestamp);
                } catch (NumberFormatException e) {
                    // Skip invalid rows
                }
            }
        }
    }

    // Reducer Class
    public static class CryptoReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private PriorityQueue<SymbolVolumeTimestampPair> topCurrencies;

        @Override
        protected void setup(Context context) {
            // PriorityQueue to hold top 10 cryptocurrencies sorted by total volume
            topCurrencies = new PriorityQueue<>(10, (a, b) -> Double.compare(a.getTotalVolume(), b.getTotalVolume()));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalVolume = 0;
            long highestVolumeTimestamp = 0;
            double highestVolume = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                double volume = Double.parseDouble(parts[0]);
                long timestamp = Long.parseLong(parts[1]);

                totalVolume += volume;

                if (volume > highestVolume) {
                    highestVolume = volume;
                    highestVolumeTimestamp = timestamp;
                }
            }

            // Add to priority queue
            SymbolVolumeTimestampPair pair = new SymbolVolumeTimestampPair(key.toString(), totalVolume, highestVolumeTimestamp);
            topCurrencies.add(pair);

            // Maintain only the top 10 entries
            if (topCurrencies.size() > 10) {
                topCurrencies.poll(); // Remove the smallest volume entry
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Convert the priority queue to a sorted list
            List<SymbolVolumeTimestampPair> sortedList = new ArrayList<>(topCurrencies);
            sortedList.sort((a, b) -> Double.compare(b.getTotalVolume(), a.getTotalVolume()));

            // Emit top 10 cryptocurrencies
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (SymbolVolumeTimestampPair pair : sortedList) {
                String formattedDate = sdf.format(new Date(pair.getHighestVolumeTimestamp() * 1000)); // Convert to milliseconds
                result.set("Total Volume: " + pair.getTotalVolume() + ", Highest Volume at: " + formattedDate);
                context.write(new Text(pair.getSymbol()), result);
            }
        }
    }

    // Helper Class to Store Symbol-Volume-Timestamp Data
    public static class SymbolVolumeTimestampPair {
        private final String symbol;
        private final double totalVolume;
        private final long highestVolumeTimestamp;

        public SymbolVolumeTimestampPair(String symbol, double totalVolume, long highestVolumeTimestamp) {
            this.symbol = symbol;
            this.totalVolume = totalVolume;
            this.highestVolumeTimestamp = highestVolumeTimestamp;
        }

        public String getSymbol() {
            return symbol;
        }

        public double getTotalVolume() {
            return totalVolume;
        }

        public long getHighestVolumeTimestamp() {
            return highestVolumeTimestamp;
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CryptoAnalysis <input> <output> <task>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Crypto Analysis");
        job.setJarByClass(CryptoAnalysis.class);

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        String task = args[2];
        if ("volatility".equalsIgnoreCase(task)) {
            job.setMapperClass(VolatilityMapper.class);
            job.setReducerClass(VolatilityReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        } else if ("performance".equalsIgnoreCase(task)) {
            job.setMapperClass(PerformanceMapper.class);
            job.setReducerClass(PerformanceReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        } else if ("top10cryptocurrencies".equalsIgnoreCase(task)) {
            job.setMapperClass(CryptoMapper.class);
            job.setReducerClass(CryptoReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } else {
            System.err.println("Invalid task. Use 'volatility', 'performance', or 'top10cryptocurrencies'.");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
