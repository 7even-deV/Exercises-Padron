import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object sparkScala extends App {

  val fileNameCSV = "Rango_Edades_Seccion_202205.csv"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("sparkScala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val padron = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .option("emptyValue", 0)
    .option("delimiter", ";")
    .load("src\\main\\resources\\" + fileNameCSV)

  val padron2 = padron
    .withColumn("DESC_DISTRITO", trim(col("desc_distrito")))
    .withColumn("DESC_BARRIO", trim(col("desc_barrio")))

  val padron_mal = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .option("delimiter", ";")
    .load("src\\main\\resources\\" + fileNameCSV)

  val padron_cambiado = padron_mal.na
    .fill(value=0)
    .withColumn("DESC_DISTRITO", trim(col("desc_distrito")))
    .withColumn("DESC_BARRIO", trim(col("desc_barrio")))

  padron2.select(countDistinct("desc_barrio")).alias("n_barrios").show()

  padron_cambiado.createOrReplaceTempView("padron")

  spark.sql("SELECT COUNT(distinct(desc_barrio)) FROM padron").show()

  val padron3 = padron_cambiado.withColumn("longitud", length(col("desc_distrito")))
  padron3.show()

  val padron4 = padron3.withColumn("valor5", lit(5))
  padron4.show()

  val padron5 = padron4.drop(col("valor5"))
  padron5.show()

  val padron_particionado = padron5.repartition(col("DESC_DISTRITO"), col("DESC_BARRIO"))

  padron_particionado.cache()

  padron_particionado.groupBy(col("desc_barrio"), col("desc_distrito"))
    .agg(count(col("espanolesHombres")).alias("espanolesHombres"),
      count(col("espanolesMujeres")).alias("espanolesMujeres"),
      count(col("extranjerosHombres")).alias("extranjerosHombres"),
      count(col("extranjerosMujeres")).alias("extranjerosMujeres"))
    .orderBy(desc("extranjerosMujeres"),desc("extranjerosHombres"))
    .show()

  spark.catalog.clearCache()

  val df1 = padron_particionado.select(col("DESC_BARRIO"), col("DESC_DISTRITO"), col("ESPANOLESHOMBRES"))
    .groupBy(col("DESC_BARRIO"),col("DESC_DISTRITO"))
    .agg(sum(col("ESPANOLESHOMBRES")).alias("ESPANOLESHOMBRES"))
    .join(padron, padron_particionado("desc_barrio") === padron("desc_barrio")
        && padron_particionado("desc_distrito") === padron("desc_distrito"), "inner")
    .show()

  val res = df1.join(padron, padron("DESC_BARRIO") === df1("DESC_BARRIO")
            && padron("DESC_DISTRITO") === df1("DESC_DISTRITO"), "inner")
  res.show()

  val padron_ventana = padron.withColumn("TotalEspHom", sum(col("espanoleshombres"))
    .over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")))
  padron_ventana.show()

  val distritos = Seq("BARAJAS","CENTRO","RETIRO")

  val padron_pivot = padron_particionado
    .groupBy("cod_edad_int")
    .pivot("desc_distrito", distritos)
    .sum("espanolesMujeres")
    .orderBy(col("cod_edad_int"))
  padron_pivot.show()

  val padron_porcen = padron_pivot.withColumn("porcentaje_barajas", round(col("barajas")
                      / (col("barajas")+ col("centro") + col("retiro")) * 100, 2))
    .withColumn("porcentaje_centro", round(col("centro") /
            (col("barajas") + col("CENTRO") + col("RETIRO")) * 100, 2))
    .withColumn("porcentaje_retiro", round(col("retiro") /
            (col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
  padron_porcen.show()

  padron.write.format("csv")
    .option("header", "true")
    .mode("overwrite")
    .partitionBy("desc_distrito", "desc_barrio")
    .save("src\\main\\resources")

  padron.write.format("parquet")
    .mode("overwrite")
    .partitionBy("desc_distrito", "desc_barrio")
    .save("src\\main\\resources")
}
