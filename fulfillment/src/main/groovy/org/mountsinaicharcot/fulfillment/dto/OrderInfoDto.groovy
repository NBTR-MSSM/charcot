package org.mountsinaicharcot.fulfillment.dto

import groovy.transform.ToString

@ToString(includeNames=true)
class OrderInfoDto {
  String orderId
  List<String> fileNames
  List<String> filesProcessed
  String email
  String outputPath
  String status
  String remark = null
  String sqsReceiptHandle = null
  Long size = 0
  Map<Integer, List<String>> bucketToFileList
}
