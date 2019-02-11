import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WebLogSessionization {

    /**
     * Constants used in the project:
     * 1) Date format ISO_8601 to convert string to date
     * 2) Assume session time window is 15 minutes
     * 3) Regex pattern to parse log entries
     **/
    private static final String ISO_8601_24H_FULL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    private static final long timeout = 15 * 60 * 1000;

    private static final Pattern LogRegex = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\d{3}Z\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/.+"
    );

    /**
     * function used to match log entries according to defined regex pattern
     **/
    private static String findInLine(String line) {

        if (LogRegex.matcher(line).matches()) {
            return line;
        }
        return "invalid";
    }

    /**
     * function used to extract columns that we care about, such as IP, timestamp, url
     **/
    private static Tuple2<String, List<Tuple3<Date, Date, List<String>>>> extractKey(String line) {

        SimpleDateFormat dateFormat = new SimpleDateFormat(ISO_8601_24H_FULL_FORMAT);
        Matcher m = LogRegex.matcher(line);
        if (m.find()) {
            String ip = m.group(2);
            try {
                Date requestTimestamp = dateFormat.parse(m.group(1));
                String url = m.group(3);
                ArrayList<String> urlList = new ArrayList<>();
                urlList.add(url);
                ArrayList<Tuple3<Date, Date, List<String>>> hitList = new ArrayList<>();
                hitList.add(new Tuple3<>(requestTimestamp, requestTimestamp, urlList));
                return new Tuple2<>(ip, hitList);
            } catch (ParseException e) {
                e.printStackTrace();
                return new Tuple2<>(null, null);
            }
        }
        return new Tuple2<>(null, null);
    }

    /**
     * function used in Spark reduce phase to sessionize web logs
     **/
    private static List<Tuple3<Date, Date, List<String>>> sessionize(List<Tuple3<Date, Date, List<String>>> a,
                                                               List<Tuple3<Date, Date, List<String>>> b) {
        Tuple3<Date, Date, List<String>> nextHit = b.get(0);
        String nextHitUrl = nextHit._3().get(0);
        long nextHitTime = nextHit._1().getTime();

        int index = 0;
        List<Tuple3<Date, Date, List<String>>> sessionList = new ArrayList<>(a);
        for (Tuple3<Date, Date, List<String>> prevHit : a) {
            long startTime = prevHit._1().getTime();
            long endTime = prevHit._2().getTime();

//            ArrayList<Tuple3<Date, Date, List<String>>> sessionList = new ArrayList<>();
            ArrayList<String> urlList = new ArrayList<>( prevHit._3());
            if (!urlList.contains(nextHitUrl)) {
                urlList.add(nextHitUrl);
            }

            if (nextHitTime > endTime && (nextHitTime - endTime < timeout)) {
                sessionList.set(index,new Tuple3<>(new Date(startTime), new Date(nextHitTime), urlList));
                return sessionList;
            } else if (nextHitTime < startTime && (startTime - nextHitTime < timeout)) {
                sessionList.set(index,new Tuple3<>(new Date(nextHitTime), new Date(endTime), urlList));
                return sessionList;
            } else if (nextHitTime >= startTime && nextHitTime <= endTime) {
                sessionList.set(index,new Tuple3<>(new Date(startTime), new Date(endTime), urlList));
                return sessionList;
            }
            index++;
        }

        a.add(nextHit);

        return a;

    }


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Web Log Sessionization")
                .setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        /*Sessionize the web log by IP*/
        JavaRDD<String> rawLog = sparkContext.textFile("web_log.log"); /*Import data*/
        JavaPairRDD<String, Tuple4<Date,Date,Long,List<String>>> sessionList = rawLog
                .map(s -> findInLine(s))    /*Match log regex pattern*/
                .filter(s -> !s.equals("invalid"))  /*Remove unmatched log entries*/
                .mapToPair(s -> extractKey(s))  /*Extract IP, timestamp, url*/
                .reduceByKey((a, b) -> sessionize(a,b)) /*Perform sessionization, if a hit is in the same session, it gets merged into the session*/
                .flatMapValues(
                        (Function<List<Tuple3<Date, Date, List<String>>>, Iterable<Tuple4<Date, Date, Long, List<String>>>>) tuple3s -> {
                            ArrayList<Tuple4<Date, Date, Long, List<String>>> newTupleList = new ArrayList<>();
                            for (Tuple3<Date, Date, List<String>> i : tuple3s) {
                                Long duration = (i._2().getTime() - i._1().getTime()) / 1000;
                                newTupleList.add(new Tuple4<>(i._1(),i._2(), duration , i._3()));
                            }
                            return newTupleList;
                        })  /*Flatten sessionized results, so each row is an unique session*/
                .filter(s -> s._2()._3()>0) /*Filter out single hit*/
                .cache()
                ;

         /*Determine the average session time*/
        JavaRDD<Long> sessionTime = sessionList.map(s -> s._2._3());
        Long totalSessionTime = sessionTime.reduce((a,b) -> a + b);

        /*Determine unique URL visits per session*/
        JavaPairRDD<String, Integer> uniqueVisits = sessionList.mapValues(
                s -> s._4().size()
        );

        /*Find the most engaged users, ie the IPs with the longest session times*/
        JavaPairRDD<Long,String> engagedUsers = sessionList
                .mapToPair(s -> new Tuple2<>(s._2._3(),s._1()))
                .sortByKey(false)
                ;


        /*Results*/
        System.out.println("Number of lines in file: " + rawLog.count());
//        sessionList.saveAsTextFile("./tmp/sessionList");
        System.out.println("Average session time: " + totalSessionTime/sessionList.count() + " seconds");
//        uniqueVisits.saveAsTextFile("./tmp/uniqueVisits");
//        engagedUsers.saveAsTextFile("./tmp/engagedUsers");
        System.out.println("Top 10 most engaged users: " + engagedUsers.take(10));
    }
}