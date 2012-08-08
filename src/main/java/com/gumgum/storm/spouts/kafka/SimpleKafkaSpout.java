/**
* Copyright 2012 GumGum Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gumgum.storm.spouts.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


import com.gumgum.kafka.consumer.KafkaTemplate;
import com.gumgum.kafka.consumer.MessageCallback;

/**
 * Zookeeper based simple kafka spout.
 * @author Vaibhav Puranik
 * @version $Id:AdManagerImpl.java 475 2008-09-10 19:51:26Z vaibhav $
 */
public class SimpleKafkaSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
    private static final Log LOGGER = LogFactory.getLog("com.gumgum.storm.spouts.kafka.SimpleKafkaSpout");
    private KafkaTemplate kafkaTemplate;
    private SpoutOutputCollector collector;
    private String topic;


    public SimpleKafkaSpout(String topic, Integer batchSize) {
        this.topic = topic;
        this.batchSize = batchSize;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }


    public void nextTuple() {
        kafkaTemplate.executeWithBatch(topic, batchSize, new MessageCallback() {
            @Override
            public void processMessage(String message) throws Exception {
		// Add your message processing logic here
            }  
        });
    }

   

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare your fields here
    }

    @Override
    public void close() {
        kafkaTemplate.shutdown();
    }

    @Override
    public void activate() {
        LOGGER.info("activate called");

    }

    @Override
    public void deactivate() {
        kafkaTemplate.shutdown();

    }

    @Override
    public void ack(Object msgId) {
        LOGGER.info("ack called");

    }

    @Override
    public void fail(Object msgId) {
        LOGGER.info("fail called");

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}

