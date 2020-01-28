import java.util.*;
import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaRDD; 
import org.apache.spark.api.java.function.Function2; 
import org.apache.spark.sql.SparkSession; 
import scala.Tuple2;

public class PageRankTaxation {

    private static class Add implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaPageRankTaxation")
                .getOrCreate();


        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaPairRDD<String, String> links = lines.mapToPair(s -> {
            String[] splitup = s.split(":");
            return new Tuple2<>(splitup[0], splitup[1]);
        }).distinct().cache();

 
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

     
            for (int itr = 0; itr < 25; itr++) {

                JavaPairRDD<String, Double> contribution = links.join(ranks).values().flatMapToPair(s -> {

                    String[] LinksSeen = s._1().split(" ");
                    int noOfLinks = LinksSeen.length;
                    List<Tuple2<String, Double>> itrResult = new ArrayList<>();
                    for (String n : LinksSeen) {
                        itrResult.add(new Tuple2<>(n, s._2() / noOfLinks));
                        //System.out.println(itrResult);
                    }
                    return itrResult.iterator();
                });


                ranks = contribution.reduceByKey(new Add()).mapValues(vi -> 0.15 + vi * 0.85);
            }
               

                JavaPairRDD<String, String> SortedTitleFile = spark.read().textFile(args[1]).javaRDD()
                        .zipWithIndex().mapToPair(x -> new Tuple2<>(String.valueOf((x._2() + 1)), x._1()));
                
                JavaRDD<Tuple2<String, Double>> PR_with_title = SortedTitleFile.join(ranks).values();

               
                JavaPairRDD<Double, String> swapkv = PR_with_title.mapToPair(s ->
                        new Tuple2<>(s._2(), s._1())).sortByKey(false);

                    JavaPairRDD<String, Double> finalPageRank = swapkv.mapToPair(s -> new Tuple2<>(s._2(), s._1()));
                   
                List<Tuple2<String, Double>> finalPR = finalPageRank.collect();
                Iterator<Tuple2<String, Double>> itr = finalPR.iterator();
                int j=0;
                while (itr.hasNext()) //prints top 10
                {
                    if(j<10)
                    {
                        System.out.println(itr.next());
                       
                        j++;
                    }
                }

                spark.stop();
            }
        }
