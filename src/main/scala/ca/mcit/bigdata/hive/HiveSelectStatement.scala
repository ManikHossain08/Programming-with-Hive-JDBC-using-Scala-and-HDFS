package ca.mcit.bigdata.hive

object HiveSelectStatement extends App with HiveClient {
  val rs = stmt.executeQuery("SELECT * from ext_movie")

  while (rs.next()) {
    println(s"MovieId: ${rs.getString(1)} \t Movie Title: ${rs.getString(2)}")
  }

  rs.close()
  stmt.close()
  connection.close()
}
