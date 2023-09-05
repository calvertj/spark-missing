import org.apache.spark.sql.{DataFrame, functions => F}

def mapToStructCaseInsensitive(df: DataFrame, mapColName: String): DataFrame = {
  val mapKeys = df.select(F.map_keys(F.col(mapColName)))
                  .as[Seq[String]]
                  .collect()
                  .flatten
                  .distinct

  val upperFirstLetter = (str: String) => str.substring(0, 1).toUpperCase + str.substring(1)

  val renamedStructFields = mapKeys.map { key =>
    val upperFirstLetterKey = upperFirstLetter(key)
    F.coalesce(
      F.expr(s"map_keys($mapColName)").getItem(key),
      F.expr(s"map_keys($mapColName)").getItem(upperFirstLetterKey)
    ).alias(upperFirstLetterKey)
  }

  val newDF = df.withColumn(mapColName, F.struct(renamedStructFields: _*))

  newDF
}
