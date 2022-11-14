package org.mountsinaicharcot.fulfillment.dto

class OrderInfoDto {
  String orderId
  List<String> fileNames
  List<String> filesProcessed = []
  String email
  String outputPath
  String status
}
