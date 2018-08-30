package com.company;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Random;

import java.util.Properties;

public class Main {

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws Exception{
        Random rand = new Random();
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final int sendMessageCount = 100000;
        try {
            int i = 1;
            for(int j = 0;j<15;j++) {
                for (long index = time; index < time + sendMessageCount; index++) {
                    final ProducerRecord<Long, String> record =
                            new ProducerRecord<>("inputTopic", index,
                                    "{\"id\":\"" + i++ +"\",\"message\":\"Transaction\"}");
                    RecordMetadata metadata = producer.send(record).get();
//                long elapsedTime = System.currentTimeMillis() - time;
//                System.out.printf("sent record(key=%s value=%s) " +
//                                "meta(partition=%d, offset=%d) time=%d\n",
//                        record.key(), record.value(), metadata.partition(),
//                        metadata.offset(), elapsedTime);
                }
                System.out.println(j);
                System.out.println(i);
            }

        } finally {
            producer.flush();
            producer.close();
        }
    }
}
