package com.grey.data

import java.sql.Date

object DataCaseClass {

  case class Stocks(date: Date, open: Float, high: Float, low: Float, close: Float, volume: Float)

}
