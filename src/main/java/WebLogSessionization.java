import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;


public class WebLogSessionization {

    public static final Pattern LogRegex = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z)\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP\\/.+"
    );

    public static String findInLine(String line) {
        if (LogRegex.matcher(line).matches()) {
            return line;
        }
        return "invalid";
    };

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Sessionization")
                .setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rawLog = sparkContext.textFile("2015_07_22_mktplace_shop_web_log_sample.log");
        JavaRDD<String> sessions = rawLog.map(s -> findInLine(s))
                .filter(s -> s != "invalid");

        //sessions.saveAsTextFile("/tmp/test");
        System.out.println("Number of lines in file = " + rawLog.count());
        System.out.println("Number of lines in map = " + sessions.count());
    }
}