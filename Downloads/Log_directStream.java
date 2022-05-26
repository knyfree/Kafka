package com.c****s;

import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class Log_DirectStream {

    public static String topicName="test.msg";
    public static String brokers="10.0.0.1:9092,10.0.0.2:9092";
    public static String groupId="test";
    public static String hdfsPath="wasbs://datalake-cluster@datalakestorage.blob.core.windows.net/datalake/kafka";
    public static String fileName= "";
    public static String ts= "";
    public static String hh= "";

    static {
        String OS=System.getProperty("os.name").toLowerCase();
        // OS에 따라 hadoop.home.dir 설정값 변경
        // 리눅스에서 실행할 경우 spark-submit시 java.sercurity.auth.login.config를 jaas 파일 위치로 명시해준다. 위 usage참고

        if(OS.contains("win")) {
            System.setProperty("hadoop.home.dir", Paths.get("winutils").toAbsolutePath().toString());
            System.setProperty("java.security.auth.login.config", "C:\\Users\\knyfree\Desktop");
        } else {
            System.setProperty("hadoop.home.dir", "/usr/hdp/2.6.5.3009-43/hadoop");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // TODO Auto-gererated method stub

        // SparkConfiguration yarn을 통해서 실행, AppName을 명시한다.
        SparkConf conf = new SparkConf().setAppName("Log_DirectStream").setMaster("yarn").set("spark.driver.allowMultipleContexts","true");
        // JavaStreamingContext를 5분 주기로 실행한다.
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.minutes(5));

        // kafka 설정 정보
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.mechanism", "SCRAM-SHA-256");

        Collection<String> topics = Arrays.asList(topicName);

        JavaInputStream<ConsumerRecord<String,String>> stream - KafkaUtils.createDirectStream(
            ssc,LocationStrategies,PreferConsistent(),ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams)
        );

        Date now= new Date();
        // 경로 수정
        String filePath = hdfsPath + "/" + new SimpleDateFormat("yyyyMMdd").format(now) + "/Log";

        // message에서 key, value를 return 받는다.
        JavaPairDStream<String, String> messages = stream.mapToPair(
            new PairFunction<ConsumerRecord<String, String>, String, String>(){
                public Tuple2<String, String> call(ConsumerRecord<String, String> record){
                    return new Tuple2<String, String>(record.key(), record.value());
                }
            }
        );

/*
        // Hadoop에 key, value형태로 저장하고 싶다면 아래와 같이 한다.
        JavaPairDStream<String, String> message = messages.map(new Function<Tuple2<String, String>, String>(){
            public String call(Tuple2<String, String> tuple2){
                    return tuple2._2();
            }
        });
        stream.saveAsHadoopFiles(filePath.String.class, String.class, TextOutputFormat.class);
*/

        // key, value에서 json포맷만 필요하므로 value만 사용한다 (용도에 맞게 사용)
        stream.map(raw->raw.value()).dstream().saveAsTextFiles(filePath, "");

        ssc.start();
        // 모든 task가 종료할 때 까지 처리를 block하고 종료할 때 까지 대기한다.       
        ssc.awaitTermination();
    }
}