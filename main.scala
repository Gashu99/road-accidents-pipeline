import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Road Accidents Analysis")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // ============================================
    // 1. CHARGEMENT
    // ============================================
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .csv("./src/main/scala/ressources/cleaned.csv")

    data.printSchema()
    println(s"Nombre de lignes brutes : ${data.count()}")

    // ============================================
    // 2. NETTOYAGE & TRANSFORMATION
    // ============================================
    val dfClean = data
      // Time -> timestamp, puis extraction de l'heure
      .withColumn("Time", col("Time").cast("timestamp"))
      .withColumn("hour", hour(col("Time")))
      .drop("Time")
      // Renommer Sex_of_driver -> sexe et normaliser les valeurs
      .withColumnRenamed("Sex_of_driver", "sexe")
      .withColumn("sexe",
        when(col("sexe") === "Male", "M")
          .when(col("sexe") === "Female", "F")
          .otherwise("X")
      )
      // Normaliser Age_band_of_driver
      .withColumn("Age_band_of_driver",
        when(col("Age_band_of_driver") === "Over 51", "51")
          .otherwise(col("Age_band_of_driver"))
      )

    dfClean.show(5)

    // Valeurs distinctes de chaque colonne (exploration)
    dfClean.columns.foreach(c => dfClean.select(c).distinct().show())

    // ============================================
    // 3. INDICATEURS GENERAUX
    // ============================================

    // Q1 - Nombre total d'accidents enregistres
    val nombreAccidents = dfClean.count()
    println(s"Le nombre total d'accidents est : $nombreAccidents")

    // Q2 - Doublons dans le jeu de donnees
    val countDistinct = dfClean.distinct().count()
    val nbDoublons = nombreAccidents - countDistinct
    println(s"Nombre de lignes distinctes : $countDistinct")
    println(s"Nombre de doublons : $nbDoublons")

    // Q3 - Pourcentage d'accidents impliquant des conducteurs masculins
    val pourcentageMasculins =
      (dfClean.filter(col("sexe") === "M").count().toDouble / countDistinct.toDouble) * 100
    println(f"Pourcentage d'accidents impliquant des conducteurs masculins : $pourcentageMasculins%.2f %%")

    // Q4 - Niveau d'education le plus courant
    val niveauEducationPlusFrequent = dfClean
      .groupBy("Educational_level")
      .agg(count("Educational_level").alias("nb_accidents"))
      .orderBy(desc("nb_accidents"))
    println("--- Niveau d'education le plus frequent ---")
    niveauEducationPlusFrequent.show()

    // Q5 - Nombre d'accidents par categorie de conducteur (owner, employee, etc.)
    val accidentParCategorieConducteur = dfClean
      .groupBy("Vehicle_driver_relation")
      .agg(count("Vehicle_driver_relation").alias("nb_accidents"))
      .orderBy(desc("nb_accidents"))
    println("--- Accidents par categorie de conducteur ---")
    accidentParCategorieConducteur.show()

    // Q6 - Categorie d'age contenant le plus d'accidents
    val categorieAgeContenantAccident = dfClean
      .groupBy("Age_band_of_driver")
      .agg(count("Age_band_of_driver").alias("nb_accidents"))
      .orderBy(desc("nb_accidents"))
    println("--- Categorie d'age ayant le plus d'accidents ---")
    categorieAgeContenantAccident.show()

    // Q7 - Nombre d'accidents impliquant des vehicules en marche arriere
    val nbMarcheArriere = dfClean.filter(col("Cause_of_accident") === "Moving Backward").count()
    println(s"Nombre d'accidents en marche arriere : $nbMarcheArriere")

    // Q8 - Repartition (en %) des accidents par sexe du conducteur
    val graviteParSexe = dfClean
      .groupBy("sexe")
      .count()
      .withColumn("count", col("count").cast("Double"))
      .withColumnRenamed("count", "Nombre_accident")

    val repartitionParSexe = graviteParSexe
      .withColumn("Pourcentage_accident", round((col("Nombre_accident") / countDistinct) * 100, 2))
    println("--- Repartition des accidents par sexe (%) ---")
    repartitionParSexe.show()

    // ============================================
    // 4. GRAVITE DES ACCIDENTS (UDF)
    // ============================================

    // Q9 - Recategoriser la gravite avec une UDF
    val renommerGravite = udf((valeur: String) => valeur match {
      case "1" => "pas grave"
      case "2" => "grave"
      case _   => "tres grave"
    })

    val dfGravite = dfClean.withColumn("Accident_severity", renommerGravite(col("Accident_severity")))

    // Q10 - Principales causes d'accidents tres graves
    val principalCauseAccident = dfGravite
      .groupBy("Cause_of_accident", "Accident_severity")
      .agg(count("Accident_severity").alias("nb_accidents"))
      .filter(col("Accident_severity") === "tres grave")
      .orderBy(desc("nb_accidents"))
    println("--- Principales causes d'accidents tres graves ---")
    principalCauseAccident.show()

    // ============================================
    // 5. ANALYSE TEMPORELLE (SPARK SQL + WINDOW FUNCTION)
    // ============================================

    dfClean.createOrReplaceTempView("accidents")

    // Q11 - Heure de la journee avec le plus d'accidents (toutes journees confondues)
    val heurePicAccidents = spark.sql(
      """
        |SELECT hour, nb_accidents FROM (
        |  SELECT hour, COUNT(*) as nb_accidents,
        |         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
        |  FROM accidents
        |  GROUP BY hour
        |)
        |WHERE rn = 1
        |""".stripMargin)
    println("--- Heure avec le plus d'accidents ---")
    heurePicAccidents.show()

    // Repartition complete des accidents par heure (utile pour la visualisation)
    val accidentsParHeure = spark.sql(
      """
        |SELECT hour, COUNT(*) as nb_accidents
        |FROM accidents
        |GROUP BY hour
        |ORDER BY hour
        |""".stripMargin)

    // ============================================
    // 6. EXPORT DES RESULTATS (pour visualisation)
    // ============================================

    def exportCsv(df: DataFrame, path: String): Unit = {
      df.write.mode("overwrite").option("header", "true").csv(path)
      cleanupSparkOutput(path)
    }

    // Supprime les fichiers _SUCCESS et .crc generes par Spark, ne garde que le CSV
    def cleanupSparkOutput(path: String): Unit = {
      val dir = new File(path)
      if (dir.exists() && dir.isDirectory) {
        dir.listFiles().foreach { f =>
          if (f.getName.startsWith("_") || f.getName.endsWith(".crc")) {
            f.delete()
          }
        }
      }
    }

    val outputBase = "./src/main/scala/output"

    exportCsv(niveauEducationPlusFrequent.coalesce(1), s"$outputBase/niveau_education")
    exportCsv(accidentParCategorieConducteur.coalesce(1), s"$outputBase/accidents_par_categorie_conducteur")
    exportCsv(categorieAgeContenantAccident.coalesce(1), s"$outputBase/accidents_par_age")
    exportCsv(repartitionParSexe.coalesce(1), s"$outputBase/accidents_par_sexe")
    exportCsv(principalCauseAccident.coalesce(1), s"$outputBase/causes_accidents_graves")
    exportCsv(accidentsParHeure.coalesce(1), s"$outputBase/accidents_par_heure")
    exportCsv(dfClean.coalesce(1), s"$outputBase/accidents_clean")

    println(s"Export termine. Resultats disponibles dans le dossier $outputBase")

    spark.stop()
  }
}