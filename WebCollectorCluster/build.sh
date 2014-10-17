#!/bin/bash
#mvn package
mvn assembly:assembly

hadoop jar /home/hu/mygit/WebCollector2/WebCollectorCluster/target/WebCollectorCluster-2.0-jar-with-dependencies.jar cn.edu.hfut.dmic.webcollectorcluster.crawler.Crawler
#/home/hu/apache/hadoop-1.2.1/bin/hadoop jar /home/hu/mygit/WebCollector2/WebCollectorCluster/target/WebCollectorCluster-2.0-jar-with-dependencies.jar cn.edu.hfut.dmic.webcollectorcluster.crawler.Crawler

