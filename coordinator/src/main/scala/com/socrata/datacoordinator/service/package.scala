package com.socrata.datacoordinator

package object service {
  type HostAndPort = (String => Option[(String, Int)])
}
