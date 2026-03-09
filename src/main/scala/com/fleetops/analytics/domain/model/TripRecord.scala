package com.fleetops.analytics.domain.model

import java.time.LocalDateTime

case class TripRecord(
  vendorId: Option[Int],
  pickupDatetime: Option[LocalDateTime],
  dropoffDatetime: Option[LocalDateTime],
  pickupLocationId: Option[Int],
  dropoffLocationId: Option[Int],
  passengerCount: Option[Long],
  tripDistance: Option[Double],
  paymentType: Option[Long],
  fareAmount: Option[Double],
  tipAmount: Option[Double],
  totalAmount: Option[Double],
  tripDurationSeconds: Option[Long]
)
