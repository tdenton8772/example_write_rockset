package com.rocksetps.examplewrite;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.rockset.client.RocksetClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.producer.KafkaProducer;

public class Main {
    private static Properties m_props;
    private static Properties kafkaProps;
    private static File propFile;
    private static File kafkaPropFile;

    public static void main(String[] args) throws IOException, InterruptedException {

        //load properties files
        m_props = new Properties();
        kafkaProps = new Properties();
        propFile = new File("configuration.properties");
        kafkaPropFile = new File("kafka.properties");
        try {
            LoadProperties(propFile);
            LoadKafkaProperties(kafkaPropFile);
        } catch (Exception e) {
            System.out.println(e);
        }

        // initialize rockset and kafka
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProps);
        RocksetClient client = new RocksetClient(m_props.getProperty("API_KEY"), m_props.getProperty("API_SERVER"));

        // start creating threads and writing data to rockset
        WriteRockset write = new WriteRockset(client, m_props, producer);
        ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(m_props.getProperty("write_threads")));
        while(true) {
            Thread newThread = new Thread(write);
            executor.execute(newThread);
        }
    }

        public static void LoadProperties(File f) throws IOException {
            FileInputStream propStream = null;
            propStream = new FileInputStream(f);
            m_props.load(propStream);
            propStream.close();
        }

        public static void LoadKafkaProperties(File f) throws IOException {
            FileInputStream kafkaPropStream = null;
            kafkaPropStream = new FileInputStream(f);
            kafkaProps.load(kafkaPropStream);
            kafkaPropStream.close();
        }

}
