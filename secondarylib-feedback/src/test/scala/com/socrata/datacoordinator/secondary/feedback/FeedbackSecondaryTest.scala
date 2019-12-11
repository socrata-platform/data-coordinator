package com.socrata.datacoordinator.secondary.feedback

import com.rojoma.json.v3.ast.JValue
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.secondary.feedback.TestData._
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}



class FeedbackSecondaryTest extends WordSpec with Matchers with MockFactory {
  def typeFromJValue(jVal: JValue) = None

  def fromJValueFunc(typ: SoQLType) = { _: JValue => None }

  abstract class TestDataCoordinatorClient extends DataCoordinatorClient[SoQLType, SoQLValue](typeFromJValue, "test", fromJValueFunc)

  def withMockDC[T](name: String,
                    feedbackCookie: Option[FeedbackCookie] = None,
                    expectations: DataCoordinatorClient[SoQLType, SoQLValue] => Unit = expectNoDataCoordinatorCalls)(f: (Secondary[SoQLType, SoQLValue], Cookie) => T): Unit = {
    name in {
      val mockDC = mock[TestDataCoordinatorClient]
      expectations(mockDC)
      val secondary = new AdditionSecondary(mockDC)
      f(secondary, feedbackCookie.map(FeedbackCookie.encode).flatten)
    }
  }

  def expectNoDataCoordinatorCalls(mockDC: DataCoordinatorClient[SoQLType, SoQLValue]): Unit = {
    (mockDC.exportRows _).expects(*, *, *).never
    (mockDC.postMutationScript _).expects(*, *).never
  }

  def expectResyncException[T](f: T): ResyncSecondaryException = intercept[ResyncSecondaryException] {
    f
  }

  def shouldBe(actual: Cookie, expected: Option[FeedbackCookie]): Unit = {
    FeedbackCookie.decode(actual) should be(expected)
  }

  def columns(sourceColumns: ColumnInfo[SoQLType]*): Seq[UserColumnId] = {
    Seq(systemId.id) ++ sourceColumns.map(_.id)
  }

  "FeedbackSecondary.shutdown()" when {
    "called" should {
      withMockDC("shutdown the secondary") { case (secondary, _) =>
        secondary.shutdown()
      }
    }
  }

  "FeedbackSecondary.dropDataset(.)" when {
    "called" should {
      withMockDC("drop the dataset by doing nothing") { case (secondary, cookie) =>
        secondary.dropDataset("test", cookie)
      }
    }
  }

  "FeedbackSecondary.currentVersion(.)" when {
    "called with no cookie" should {
      withMockDC("return 0") { case (secondary, cookie) =>
        secondary.currentVersion("test", cookie) should be(0)
      }
    }
    "called with a valid cookie" should {
      withMockDC("return the version in the cookie", TestCookie.v10) { case (secondary, cookie) =>
        secondary.currentVersion("test", cookie) should be(10)
      }
    }
  }

  "FeedbackSecondary.currentCopyNumber(.)" when {
    "called with no cookie" should {
      withMockDC("return 0") { case (secondary, cookie) =>
        secondary.currentCopyNumber("test", cookie) should be(0)
      }
    }
    "called with a valid cookie" should {
      withMockDC("return the copy number in the cookie", TestCookie.v10) { case (secondary, cookie) =>
        secondary.currentCopyNumber("test", cookie) should be(1)
      }
    }
  }

  "FeedbackSecondary.version(.)" when {
    // tests for logic around cookie
    "called with no cookie" should {
      withMockDC("throw a ResyncSecondaryException") { case (secondary, cookie) =>
        val caught = intercept[ResyncSecondaryException] {
          secondary.version(datasetInfo, 10, cookie, Iterator.empty)
        }
        caught.reason should be("No cookie value for dataset test")
      }
    }
    "called with an invalid cookie" should {
      withMockDC("throw a ResyncSecondaryException") { case (secondary, _) =>
        var caught = intercept[ResyncSecondaryException] {
          secondary.version(datasetInfo, 10, Some("not a valid cookie"), Iterator.empty)
        }
        caught.reason should be("No cookie value for dataset test")

        caught = intercept[ResyncSecondaryException] {
          secondary.version(datasetInfo, 10, Some("{\"not_a\":\"valid cookie\"}"), Iterator.empty)
        }
        caught.reason should be("No cookie value for dataset test")
      }
    }
    "called for the current version" should {
      withMockDC("do nothing", TestCookie.v10) { case (secondary, cookie) =>
        secondary.version(datasetInfo, 10, cookie, Iterator.empty) should be(cookie)
      }
    }
    "called for a previous version" should {
      withMockDC("do nothing", TestCookie.v10) { case (secondary, cookie) =>
        secondary.version(datasetInfo, 8, cookie, Iterator.empty) should be(cookie)
      }
    }
    "called for a version past the next" should {
      withMockDC("throw a ResyncSecondaryException", TestCookie.v10) { case (secondary, cookie) =>
        val caught = intercept[ResyncSecondaryException] {
          secondary.version(datasetInfo, 12, cookie, Iterator.empty)
        }
        caught.reason should be("Unexpected data version 12")
      }
    }
    "called for version 1 with no cookie" should {
      withMockDC("succeed and return the expected cookie", TestCookie.v1) { case (secondary, cookie) =>
        secondary.version(datasetInfo, 1, None, TestEvent.v1Events) should be(cookie)
      }
    }

    // tests around exporting row data
    "called for a version with a new computed column exporting no row data" should {
      withMockDC("export no rows and return the expected cookie", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.systemPKEmpty)
        (mockDC.postMutationScript _).expects(*, *).never
      }) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events), TestCookie.v3)
      }
    }
    "called for a version with a new computed column with unexpected error with DC client" should {
      withMockDC("should throw an Exception", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.unexpectedError)
        (mockDC.postMutationScript _).expects(*, *).never
      }) { case (secondary, cookie) =>
        val caught = intercept[Exception] {
          secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events)
        }
        caught.getMessage should be("Unexpected error in data-coordinator client: test")
      }
    }
    "called for a version with a new computed column and failed to discover DC" should {
      withMockDC("throw a ReplayLaterException", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.failedToDiscoverDC)
        (mockDC.postMutationScript _).expects(*, *).never
      }) { case (secondary, cookie) =>
        val caught = intercept[ReplayLaterSecondaryException] {
          secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events)
        }
        caught.reason should be(FailedToDiscoverDataCoordinator.english)
        val expectedCookie = FeedbackCookie.encode(TestCookie.v2.get.copy(errorMessage = Some(FailedToDiscoverDataCoordinator.english)))
        caught.cookie should be(expectedCookie)
      }
    }
    "called for a version with a new computed column and dataset busy" should {
      withMockDC("throw a ReplayLaterException", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.dataCoordinatorBusy)
        (mockDC.postMutationScript _).expects(*, *).never
      }) { case (secondary, cookie) =>
        val caught = intercept[ReplayLaterSecondaryException] {
          secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events)
        }
        caught.reason should be(DataCoordinatorBusy.english)
        val expectedCookie = FeedbackCookie.encode(TestCookie.v2.get.copy(errorMessage = Some(DataCoordinatorBusy.english)))
        caught.cookie should be(expectedCookie)
      }
    }
    "called for a version with a new computed column and it and the source column have been deleted" should {
      withMockDC("return the expected cookie", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.num1DoesNotExist)
        (mockDC.postMutationScript _).expects(*, *).never
      }) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events), TestCookie.v3almost5)
      }
    }

    // tests around posting mutation scripts
    "called for a version with a new computed column exporting row data" should {
      withMockDC("export rows in batches and return the expected cookie", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.systemPKNum1Num2)
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(0), TestCookie.v3Schema).once.returning(TestScripts.success)
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(1), TestCookie.v3Schema).once.returning(TestScripts.success)
      }) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events), TestCookie.v3)
      }
    }
    "called for a version with a new computed column target column deleted before post" should {
      withMockDC("export rows in batches and return the expected cookie", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.systemPKNum1Num2)
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(0), TestCookie.v3Schema).once.returning(TestScripts.success)
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(1), TestCookie.v3Schema).once.returning(TestScripts.sum12DoesNotExist)
      }) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events), TestCookie.v3almost4)
      }
    }
    "called and receives an unexpected error when posting a mutation script" should {
      withMockDC("throw a ReplayLaterException", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.systemPKNum1Num2)
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(0), TestCookie.v3Schema).once.returning(TestScripts.unexpectedError)
      }) { case (secondary, cookie) =>
        val caught = intercept[ReplayLaterSecondaryException] {
          secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events)
        }
        caught.reason should be("test")
        // for this the data-coordinator retries should be decremented
        val expectedCookie = FeedbackCookie.encode(TestCookie.v2.get.copyCurrent(dataCoordinatorRetriesLeft = 4, errorMessage = Some("test")))
        caught.cookie should be(expectedCookie)
      }
    }
    "called and failed to discover DC when posting a mutation script" should {
      withMockDC("throw a ReplayLaterException", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.systemPKNum1Num2)
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(0), TestCookie.v3Schema).once.returning(TestScripts.failedToDiscoverDC)
      }) { case (secondary, cookie) =>
        val caught = intercept[ReplayLaterSecondaryException] {
          secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events)
        }
        caught.reason should be(FailedToDiscoverDataCoordinator.english)
        val expectedCookie = FeedbackCookie.encode(TestCookie.v2.get.copy(errorMessage = Some(FailedToDiscoverDataCoordinator.english)))
        caught.cookie should be(expectedCookie)
      }
    }
    "called and data-coordinator busy when posting a mutation script" should {
      withMockDC("throw a ReplayLaterException", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.systemPKNum1Num2)
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(0), TestCookie.v3Schema).once.returning(TestScripts.dataCoordinatorBusy)
      }) { case (secondary, cookie) =>
        val caught = intercept[ReplayLaterSecondaryException] {
          secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events)
        }
        caught.reason should be(DataCoordinatorBusy.english)
        val expectedCookie = FeedbackCookie.encode(TestCookie.v2.get.copy(errorMessage = Some(DataCoordinatorBusy.english)))
        caught.cookie should be(expectedCookie)
      }
    }
    "called and the dataset does not exist when posting a mutation script" should {
      withMockDC("succeed and return the expected cookie", TestCookie.v2, { mockDC =>
        // on flushing the new computed column, rows should be exported once
        (mockDC.exportRows _).expects(columns(num1, num2), TestCookie.v3Schema, *).once.returning(TestRows.systemPKNum1Num2)
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum12Scripts(0), TestCookie.v3Schema).once.returning(TestScripts.doesNotExist)
      }) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 3, cookie, TestEvent.v3Events), TestCookie.v3)
      }
    }

    // tests around publication / working copy creation / dropping
    "called for a version containing publish event" should {
      withMockDC("do no work and return the expected cookie", TestCookie.v6) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 7, cookie, TestEvent.v7Events), TestCookie.v7)
      }
    }
    "called for a version containing a working copy created event" should {
      withMockDC("do no work and return the expected cookie", TestCookie.v7) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 8, cookie, TestEvent.v8Events), TestCookie.v8)
      }
    }
    "called for a version containing a working copy dropped event" should {
      withMockDC("do no work and return the expected cookie", TestCookie.v8) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 9, cookie, TestEvent.v9Events), TestCookie.v9)
      }
    }

    // tests around row data updates
    "called for a version containing updates to a source column" should {
      withMockDC("post rows in batches and return the expected cookie", TestCookie.v9, { mockDC =>
        // should not export any rows
        (mockDC.exportRows _).expects(*, *, *).never
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum23ScriptsV11BatchesOf3(0), TestCookie.v10Schema).once.returning(TestScripts.success)
        (mockDC.postMutationScript _).expects(TestScripts.sum23ScriptsV11BatchesOf3(1), TestCookie.v10Schema).once.returning(TestScripts.success)
        (mockDC.postMutationScript _).expects(TestScripts.sum23ScriptsV11BatchesOf3(2), TestCookie.v10Schema).once.returning(TestScripts.success)
      }) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 10, cookie, TestEvent.v10Events), TestCookie.v10)
      }
    }
    "called for a version containing its own updates to a target computed column" should {
      withMockDC("do no work and return the expected cookie", TestCookie.v10) { case (secondary, cookie) =>
        shouldBe(secondary.version(datasetInfo, 11, cookie, TestEvent.v11Events), TestCookie.v11)
      }
    }
  }
  "FeedbackSecondary.resync(.)" when {
    "called with no cookie on the latest living copy where no updates are needed" should {
      withMockDC("do no work and return the expected cookie") { case (secondary, _) =>
        val rows = TestRows.managedRows(TestRows.num2Num3Sum23RowsV11)
        shouldBe(secondary.resync(datasetInfo, v11copyInfo, schema, None, rows, Seq.empty, isLatestLivingCopy = true), TestCookie.v11)
      }
    }
    "called with random string cookie on the latest living copy where no updates are needed" should {
      withMockDC("do no work and return the expected cookie") { case (secondary, _) =>
        val rows = TestRows.managedRows(TestRows.num2Num3Sum23RowsV11)
        shouldBe(secondary.resync(datasetInfo, v11copyInfo, schema, Some("random string"), rows, Seq.empty, isLatestLivingCopy = true), TestCookie.v11)
      }
    }
    "called with random string cookie on the latest living copy where updates are needed" should {
      withMockDC("post batches to DC and return the expected cookie", None, { mockDC =>
        // should not export any rows
        (mockDC.exportRows _).expects(*, *, *).never
        // computed values should be posted in batches
        (mockDC.postMutationScript _).expects(TestScripts.sum23ScriptsV11(0), TestCookie.v10Schema).once.returning(TestScripts.success)
        (mockDC.postMutationScript _).expects(TestScripts.sum23ScriptsV11(1), TestCookie.v10Schema).once.returning(TestScripts.success)
      }) { case (secondary, _) =>
        val rows = TestRows.managedRows(TestRows.num2Num3Sum23RowsV10)
        shouldBe(secondary.resync(datasetInfo, v10copyInfo, schema, Some("random string"), rows, Seq.empty, isLatestLivingCopy = true), TestCookie.v10)
      }
    }
    "called with no cookie on not the latest living copy" should {
      withMockDC("do no work and return the expected cookie") { case (secondary, _) =>
        val rows = TestRows.managedRows(TestRows.num2Num3Sum23RowsV7)
        shouldBe(secondary.resync(datasetInfo, v7copyInfo, schema, None, rows, Seq.empty, isLatestLivingCopy = false), TestCookie.v7)
      }
    }
    "called with previous version and copy cookie on the latest living copy" should {
      withMockDC("do no work and return the expected cookie", TestCookie.v7) { case (secondary, cookie) =>
        val rows = TestRows.managedRows(TestRows.num2Num3Sum23RowsV7)
        shouldBe(secondary.resync(datasetInfo, v8copyInfo, schema, cookie, rows, Seq.empty, isLatestLivingCopy = true), TestCookie.v8NoPrevious)
      }
    }
  }
}
