package com.rocksetps.examplewrite;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import rockset.com.google.gson.Gson;

import java.util.LinkedList;

public class WriteKafka implements Runnable {

    private LinkedList<Object> list;
    private KafkaProducer producer;
    private String topic;

    public WriteKafka(LinkedList<Object> _list, KafkaProducer _producer, String _topic) {
        list = _list;
        producer = _producer;
        topic = _topic;
    }

    public void run() {
        System.out.println("Started kafka thread");
        Gson gson = new Gson();
        String jsonArray = gson.toJson(list);
        System.out.println(jsonArray);
        try{
            ProducerRecord json = new ProducerRecord<>(topic,
                    Long.toString(System.currentTimeMillis()),
                    jsonArray);
            System.out.println(producer.send(json).get());
            producer.flush();
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}