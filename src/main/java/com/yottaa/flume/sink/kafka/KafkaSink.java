/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yottaa.flume.sink.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * A simple sink which reads a event from a channel and publish to Kafka. 
 */
public class KafkaSink extends AbstractSink implements Configurable{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);
	private String topic;
	private Producer<String, String> producer;
	
	public Status process() throws EventDeliveryException {
		
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		try {
			transaction.begin();
			Event e = channel.take();
			if(e == null) {
				status = Status.BACKOFF;
			}else{
				producer.send(new KeyedMessage<String, String>(topic, new String(e.getBody())));
				LOGGER.trace("Message: {}", e.getBody());
			}
			transaction.commit();
		} catch(Exception e) {
			LOGGER.error("KafkaSink Exception:{}",e);
			transaction.rollback();
			status = Status.BACKOFF;
		} finally {
			transaction.close();
		}
		
		return status;
	}

	public void configure(Context context) {
		
		topic = context.getString("topic");
	    Preconditions.checkState(topic != null, "No Kafka topic specified");
	    
		Properties props = new Properties();
		Map<String, String> contextMap = context.getParameters();
		for(String key : contextMap.keySet()) {
			if (!key.equals("type") && !key.equals("channel")) {
				props.setProperty(key, context.getString(key));
				LOGGER.info("key={},value={}",key,context.getString(key));
			}
		}
		producer =  new Producer<String, String>(new ProducerConfig(props));
	}

	  @Override
	  public void start() {
		LOGGER.info("Kafka sink starting");
	    super.start();
	  }
	  

	@Override
	public synchronized void stop() {
		LOGGER.info("Kafka sink {} stopping", this.getName());
		producer.close();
		super.stop();
	}
}
