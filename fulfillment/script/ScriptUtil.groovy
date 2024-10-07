import static ch.qos.logback.classic.Level.ERROR

import org.slf4j.LoggerFactory

// Recipe for this include style script found at https://stackoverflow.com/a/50463439/2434307

// Increase log level to reduce chatter in the logs of these guys
[
  'org.apache.http',
  'software.amazon.awssdk',
  'groovyx.net.http',
  'com.amazonaws'
].each {
  LoggerFactory.getILoggerFactory().getLogger(it).setLevel(ERROR)
}

def logger(Object obj) {
  LoggerFactory.getLogger(obj.getClass())
}
