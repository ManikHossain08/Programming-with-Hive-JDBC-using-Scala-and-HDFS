package ca.mcit.bigdata.hive

import org.apache.hive.jdbc.HiveDriver

import java.sql.DriverManager

trait HiveClient {
  val driverName: String = classOf[HiveDriver].getName
  Class.forName(driverName)

  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/bdsf2001_manik;user=manik;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()

}
