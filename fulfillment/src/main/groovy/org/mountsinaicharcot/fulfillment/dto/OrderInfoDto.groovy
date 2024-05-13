package org.mountsinaicharcot.fulfillment.dto

import groovy.transform.Memoized
import groovy.transform.ToString

@ToString(includeNames = true, ignoreNulls = true)
class OrderInfoDto {
  String orderId
  Integer recordNumber
  List<String> fileNames
  List<String> filesProcessed
  String email
  String filter
  String outputPath
  String status
  String remark = null
  String sqsReceiptHandle = null
  Long size = 0
  Map<Integer, List<String>> bucketToFileList
  Integer lastRecordNumber

  @Memoized
  Integer approximateItemSizeInBytes() {
    (orderId + fileNames?.join(',') + remark + status + email + sqsReceiptHandle + filesProcessed?.join(',')).getBytes('UTF-8').length
  }
}
