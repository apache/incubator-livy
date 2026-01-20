package org.apache.livy.server.interactive

import org.scalatest.FunSpec
import org.scalatest.Matchers.{be, noException}

class CreateInteractiveRequestSpec extends FunSpec {
  describe("CreateInteractiveRequest redaction and maskedConfString") {
    it("does not redact when no regex is provided") {
      val req = new CreateInteractiveRequest
      req.conf = Map("password" -> "secret", "user" -> "alice")
      val masked = req.toString
      assert(masked.contains("password=secret"))
      assert(masked.contains("user=alice"))
    }

    it("redacts values when regex matches the key") {
      val req = new CreateInteractiveRequest
      req.conf = Map("spark.redaction.regex" -> "password|secret", "password" -> "hunter2", "user" -> "bob")
      val masked = req.toString
      assert(masked.contains("password=*****"))
      assert(masked.contains("user=bob"))
    }

    it("handles invalid regex gracefully and does not throw") {
      val req = new CreateInteractiveRequest
      // invalid regex pattern
      req.conf = Map("spark.redaction.regex" -> "(unclosed[", "secret" -> "value")
      // should not throw when calling toString
      noException should be thrownBy req.toString
    }
  }
}


