#!/bin/bash

APP_NAME="hadoop-weather-02"
APP_MAIN="de.dimajix.training.hadoop.weather.Weather"
APP_VERSION="0.1.0"
CDH_VERSION="cdh5.5.0"

JAR_NAME="target/$APP_NAME-$APP_VERSION-$CDH_VERSION-jar-with-dependencies.jar"

hadoop jar $JAR_NAME $APP_MAIN $@

