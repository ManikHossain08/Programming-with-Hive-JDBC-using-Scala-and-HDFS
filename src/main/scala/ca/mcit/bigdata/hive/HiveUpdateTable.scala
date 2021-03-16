package ca.mcit.bigdata.hive

object HiveUpdateTable extends App with HiveClient {

  stmt.executeUpdate(
    """CREATE TABLE if not exists enriched_rating(
      | mid int,
      | title string,
      | rid int,
      | starts int,
      | ratingDate string
      |)""".stripMargin)


  val hasResultSet = stmt.execute(
    """INSERT OVERWRITE TABLE enriched_rating
      |SELECT m.mid, title, rid, stars, ratingdate
      |FROM ext_movie m JOIN ext_rating r ON m.mid = r.mid""".stripMargin)

  if (hasResultSet) println("I received a result set")
  else println("It was an update query and I haven't received a result set")

  val hasResultSet2 = stmt.execute("select distinct mid, title from enriched_rating_sequence")
  if (hasResultSet2) {
    val rs = stmt.getResultSet
    while (rs.next()) {
      println(s"MovieId: ${rs.getString(1)} \t Movie Title: ${rs.getString(2)}")
    }
    rs.close()
  }

  stmt.close()
  connection.close()
}
