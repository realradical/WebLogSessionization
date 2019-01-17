import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WebLogSessionization {

    private static final String ISO_8601_24H_FULL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private static final Pattern LogRegex = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z)\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/.+"
    );

    private static String findInLine(String line) {

        if (LogRegex.matcher(line).matches()) {
            return line;
        }
        return "invalid";
    }

    private static Tuple2<String, Tuple3<Date, Date, List<String>>> extractKey(String line) {

        SimpleDateFormat dateFormat = new SimpleDateFormat(ISO_8601_24H_FULL_FORMAT);
        Matcher m = LogRegex.matcher(line);
        if (m.find()) {
            String ip = m.group(2);
            try {
                Date requestTimestamp = dateFormat.parse(m.group(1));
                String url = m.group(3);
                ArrayList urlList = new ArrayList<String>();
                urlList.add(url);
                return new Tuple2<>(ip, new Tuple3<>(requestTimestamp, requestTimestamp, urlList));
            } catch (ParseException e) {
                e.printStackTrace();
                return new Tuple2<>(null, null);
            }
        }
        return new Tuple2<>(null, null);
    }

    private static Tuple3<Date, Date, List<String>> sessionize(Tuple3<Date, Date, List<String>> a,
                                                               Tuple3<Date, Date, List<String>> b) {
        a._3().add(b._3().get(0));
        return new Tuple3<>(a._1(),b._1(),a._3());
    }


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Sessionization")
                .setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rawLog = sparkContext.textFile("2015_07_22_mktplace_shop_web_log_sample.log");
        JavaPairRDD<String,Tuple3<Date, Date, List<String>>> hits = rawLog
                .map(s -> findInLine(s))
                .filter(s -> s != "invalid")
                .mapToPair(s -> extractKey(s));
        JavaPairRDD<String,Tuple3<Date, Date, List<String>>> test = hits
                .reduceByKey((a, b) -> sessionize(a,b));


        test.saveAsTextFile("./tmp/test");
        System.out.println("Number of lines in file = " + rawLog.count());
        System.out.println("Number of lines in map = " + test.count());
    }
}