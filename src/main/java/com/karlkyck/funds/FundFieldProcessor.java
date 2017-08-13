package com.karlkyck.funds;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by karlkyck on 13/08/17.
 */
public class FundFieldProcessor {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fund-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KStreamBuilder builder = new KStreamBuilder();

        // TODO: the following can be removed with a serialization factory
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Transaction> transactionSerializer = new JsonPOJOSerializer<>();
        serdeProps.put(JsonPOJODeserializer.PROPERTY_JSON_POJO_CLASS, Transaction.class);
        transactionSerializer.configure(serdeProps, false);

        final Deserializer<Transaction> transactionDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put(JsonPOJODeserializer.PROPERTY_JSON_POJO_CLASS, Transaction.class);
        transactionDeserializer.configure(serdeProps, false);

        final Serde<Transaction> transactionSerde = Serdes.serdeFrom(transactionSerializer, transactionDeserializer);

        final Serializer<ZippedTransactions> zippedTransactionsSerializer = new JsonPOJOSerializer<>();
        serdeProps.put(JsonPOJODeserializer.PROPERTY_JSON_POJO_CLASS, ZippedTransactions.class);
        zippedTransactionsSerializer.configure(serdeProps, false);
        final Deserializer<ZippedTransactions> zippedTransactionsDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put(JsonPOJODeserializer.PROPERTY_JSON_POJO_CLASS, ZippedTransactions.class);
        zippedTransactionsDeserializer.configure(serdeProps, false);
        final Serde<ZippedTransactions> zippedTransactionsSerde = Serdes.serdeFrom(zippedTransactionsSerializer, zippedTransactionsDeserializer);

//        KTable<Integer, Transaction> source1Transactions = builder
//                .stream(Serdes.Integer(), transactionSerde, "streams-source1-transactions-input")
//                .peek((key, value) -> System.out.println(value.id))
//                .selectKey((key, value) -> value.id)
//                .groupByKey()
//                .reduce((value1, value2) -> value2);

        KTable<Integer, Transaction> source1Transactions = builder
                .stream(transactionSerde, transactionSerde, "streams-source1-transactions-input")
                .selectKey((key, value) -> value.id)
                .groupByKey(Serdes.Integer(), transactionSerde)
                .reduce((value1, value2) -> value2);

        KTable<Integer, Transaction> source2Transactions = builder
                .stream(transactionSerde, transactionSerde, "streams-source2-transactions-input")
                .selectKey((key, value) -> value.id)
                .groupByKey(Serdes.Integer(), transactionSerde)
                .reduce((value1, value2) -> value2);

        source1Transactions
                .join(source2Transactions, ZippedTransactions::new)
                .toStream()
                .to(Serdes.Integer(), zippedTransactionsSerde, "streams-zipped-transactions-output");


        KafkaStreams streams = new KafkaStreams(builder, props);


        streams.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());


        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
//        Thread.sleep(5000L);

//        streams.close();
    }

}
