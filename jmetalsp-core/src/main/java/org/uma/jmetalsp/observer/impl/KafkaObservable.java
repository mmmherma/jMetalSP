package org.uma.jmetalsp.observer.impl;

import org.apache.kafka.clients.producer.*;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;
import org.uma.jmetalsp.ObservedData;
import org.uma.jmetalsp.observer.Observable;
import org.uma.jmetalsp.observer.Observer;
import org.uma.jmetalsp.util.serialization.DataSerializer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * @author Antonio J. Nebro <antonio@lcc.uma.es>
 */
public class KafkaObservable<O extends ObservedData<?>> implements Observable<O> {
  private Set<Observer<O>> observers;
  private boolean dataHasChanged;
  private String topicName;
  private Producer<String, String> producer;
  private Producer<Integer, byte[]> producerAVRO;
  private String pathAVRO;
  private DataSerializer serializer;
  public KafkaObservable(String topicName, O observedDataObject) {
    observers = new HashSet<>();
    dataHasChanged = false;
    this.topicName = topicName;

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    this.producer = new KafkaProducer<>(properties) ;
  }

  public KafkaObservable(String topicName,String pathAVRO) {
    this.observers = new HashSet<>();
    this.dataHasChanged = false;
    this.topicName = topicName;
    this.pathAVRO =pathAVRO;
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    this.producerAVRO = new KafkaProducer<>(properties) ;
    this.serializer = new DataSerializer();
  }

  @Override
  public void register(Observer<O> observer) {
    observers.add(observer);
  }


  @Override
  public void unregister(Observer<O> observer) {
    observers.remove(observer);
  }

  @Override
  public void notifyObservers(O data) {
    if (dataHasChanged) {
      System.out.println("Sending data") ;
      if(pathAVRO==null) {
        producer.send(new ProducerRecord<String, String>(topicName, "0", data.toJson()));
      }else{
        byte [] aux= serializer.serializeMessage(data.getData(),pathAVRO);
        int count = JMetalRandom.getInstance().nextInt(0,10000);
        Future<RecordMetadata> send =
                producerAVRO.send(new ProducerRecord<Integer, byte[]>
                        (topicName, count, aux));
      }
    }
    clearChanged();
  }

  @Override
  public int numberOfRegisteredObservers() {
    return observers.size();
  }

  @Override
  public Collection<Observer<O>> getObservers() {
    return observers ;
  }

  @Override
  public void setChanged() {
    dataHasChanged = true;
  }

  @Override
  public boolean hasChanged() {
    return dataHasChanged;
  }

  @Override
  public void clearChanged() {
    dataHasChanged = false;
  }

}
