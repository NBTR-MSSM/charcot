package org.mountsinaicharcot.fulfillment.service

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

class FulfillmentUtil {
  public static final String WORK_FOLDER = './.charcot'

  static String currentTime() {
    DateTimeZone utc = DateTimeZone.forID('GMT')
    DateTime dt = new DateTime(utc)
    DateTimeFormatter fmt = DateTimeFormat.forPattern('E, d MMM, yyyy HH:mm:ssz')
    StringBuilder now = new StringBuilder()
    fmt.printTo(now, dt)
    now.toString()
  }
}
