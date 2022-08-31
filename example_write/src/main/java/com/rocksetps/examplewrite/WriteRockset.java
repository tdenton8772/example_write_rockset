package com.rocksetps.examplewrite;

import com.rockset.client.ApiException;
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import com.rockset.client.model.AddDocumentsResponse;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;

public class WriteRockset implements Runnable {
    private RocksetClient client;
    private Properties m_props;
    private KafkaProducer producer;

    public WriteRockset(RocksetClient _client, Properties _m_props, KafkaProducer _producer) {
        client = _client;
        m_props = _m_props;
        producer = _producer;
    }

    public void run() {
        // load json from a file or create a list of objects to be inserted
        LinkedList<Object> list = new LinkedList<>();
        Map<String, Object> json = new LinkedHashMap<>();

        Random rand = new Random();
        int maxDocs = 9;
        int numDocs = rand.nextInt(maxDocs) + 1;
        for (int i = 0; i < numDocs; i++) {
            list.add(makeDoc(Integer.parseInt(m_props.getProperty("body_length"))));
        }

        AddDocumentsRequest documentsRequest = new AddDocumentsRequest().data(list);

        int count = 1;
        int maxTries = Integer.parseInt(this.m_props.getProperty("max_retries")) + 1;
        while (true) {
            try {
                AddDocumentsResponse documentsResponse =
                        client.documents.add(m_props.getProperty("workspace"),
                                m_props.getProperty("collection"),
                                documentsRequest);
                break;
            } catch (ApiException e) {
                int timeout = (int) Math.pow(2, count * 2);
                try {
                    TimeUnit.MILLISECONDS.sleep(timeout);
                } catch (InterruptedException _e) {
                    _e.printStackTrace();
                }
                System.out.println(e);
                if (++count == maxTries) {
                    writeKafka(list, producer, m_props.getProperty("topic"));
                    break;
                }
            } catch (Exception e) {
                System.out.println(e);
                break;
            }
        }

    }

    public static Map<String, Object> makeDoc(Integer body_length) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("_id", getDocId());
        json.put("body", getDocBody(body_length));
        return json;
    }

    private static String getDocId() {
        Random rand = new Random();
        int upperbound = 8999999;
        int int_random = rand.nextInt(upperbound) + 1000000;
        String docId = Integer.toString(int_random);
        return docId;
    }

    private static String getDocBody(Integer body_length) {
        String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder chars = new StringBuilder();
        Random rnd = new Random();
        while (chars.length() < body_length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * CHARS.length());
            chars.append(CHARS.charAt(index));
        }
        String charString = chars.toString();
        return charString;
    }

    private static void writeKafka(LinkedList<Object> list, KafkaProducer producer, String topic) {
        WriteKafka forward = new WriteKafka(list, producer, topic);
        Thread newThread = new Thread(forward);
        newThread.start();
    }
}
