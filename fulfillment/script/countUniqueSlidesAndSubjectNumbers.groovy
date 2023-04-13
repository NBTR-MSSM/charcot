import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request

AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion('us-east-1').build()
Set<String> slides = []
Set<String> subjects = []
Set<String> unmatched = []
String continuationToken = null
while (true) {
  def objectListing = s3.listObjectsV2([bucketName: 'nbtr-production', continuationToken: continuationToken] as ListObjectsV2Request)
  objectListing.objectSummaries.findAll {
    it.key.endsWith('.mrxs')
  }.collect(slides) {
    it.key
  }

  subjects << slides.findResults {
    def res = it =~ /^(\d+)_/
    if (res) {
      return res[0][1]
    }
    unmatched << it
    null
  }.toSet()
  if (!objectListing.isTruncated()) {
    break
  }
  continuationToken = objectListing.nextContinuationToken
}
println "${slides.size()} unique slides"
println "${subjects.size()} unique subjects"
println "${unmatched.size()} unique unmatched"
println unmatched.join('\n')
