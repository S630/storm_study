package com.storm.kafka.wordcount;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;



public class KafkaWordSplitter extends BaseRichBolt {

	private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);

	private static final long serialVersionUID = 886149197481637894L;

	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context,

			OutputCollector collector) {

		this.collector = collector;

	}

	public void execute(Tuple input) {

		String line = input.getString(0);

		LOG.info("RECV[kafka -> splitter] " + line);

		String[] words = line.split("\\s+");

		for (String word : words) {

			LOG.info("EMIT[splitter -> counter] " + word);

			collector.emit(input, new Values(word, 1));

		}

		collector.ack(input);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("word", "count"));

	}

}
