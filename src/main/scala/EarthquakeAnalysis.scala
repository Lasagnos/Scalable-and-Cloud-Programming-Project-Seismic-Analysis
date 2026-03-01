import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


object EarthquakeAnalysis {
  def main(args: Array[String]): Unit = {
    // Argomenti del main (percorsi di input e output csv). Se assenti, path locali
    val inputPath = if (args.length > 0) args(0) else "data/Datasets/dataset-earthquakes-trimmed.csv"
    val outputPath = if (args.length > 1) args(1) else "data/Outputs"

    // Inizializzazione Sessione Spark
    val spark = SparkSession.builder()
      .appName("Earthquake Application")
      // Se il master non è già definito (es. da un comando esterno), usa "local[*]"
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  // Nasconde gli 'INFO' nella console

    //import spark.implicits._  // Per il toDF usato nel salvataggio output (non più usato)

    // Caricamento Dataset come RDD
    val data = spark.read.option("header", value = true).csv(inputPath).rdd

    // PRE-PROCESSING
    // Trasformiamo ogni riga in: ((lat_arrotondata, lon_arrotondata), data_senza_ora)
    val cleanedEvents: RDD[((Double, Double), String)] = data.flatMap { row =>
      try {
        val lat = row.getAs[String]("latitude").toDouble
        val lon = row.getAs[String]("longitude").toDouble
        val timestamp = row.getAs[String]("date")

        // Arrotondamento alla prima cifra decimale
        val latRounded = BigDecimal(lat).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
        val lonRounded = BigDecimal(lon).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
        // Estrazione della data
        val dateOnly = timestamp.split(" ")(0)

        Some(((latRounded, lonRounded), dateOnly))
      } catch {
        case _: Exception => None // Qualsiasi altro tipo di riga è ignorata
      }
    }.distinct() // '.distinct' rimuove i duplicati (stessa cella, stesso giorno)


    // ====== INIZIO LOGICA PRINCIPALE ======
    // Raggruppamento per data
    val numPartitions = if (args.length > 2) args(2).toInt else 32
    val eventsByDate = cleanedEvents
      .map { case (loc, date) => (date, loc) }
      .repartition(numPartitions) // Bilancia il carico tra i nodi
      .groupByKey()

    // Generazione Coppie di Località
    // Per ogni data, creiamo tutte le combinazioni uniche di due località diverse
    val coOccurrences = eventsByDate.flatMap { case (date, locations) =>
      val locArray = locations.toArray.distinct.sorted // No duplicati e ordiniamo per evitare (A,B) e (B,A)
      if (locArray.length < 2) Seq.empty  // Non processare giornate con un solo terremoto
      else{
        for {
          i <- locArray.indices
          j <- (i + 1) until locArray.length
        } yield ((locArray(i), locArray(j)), List(date))  // ritorna ogni (coppia, date) trovata
      }
    }

    // Aggrega le liste delle date per le stesse coppie (sempre con distinct)
    val results = coOccurrences
      .reduceByKey((dates1, dates2) => (dates1 ++ dates2).distinct)
      //.cache() // I risultati vengono mantenuti in RAM per le azioni successive (.isEmpty, .max e .map)
      //.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER) // occupa meno spazio in RAM
    // Testare con e senza cache per report?

    // Risultato: un elenco di coppie univoche, ognuna associata alla lista completa di date in cui sono co-occorse
    // ====== FINE LOGICA PRINCIPALE ======


    // Ricerca della coppia con più co-occorrenze e ordinamento date
    if (!results.isEmpty()) {
      val winner = results.max()(Ordering.by(_._2.size))  // Coppia con lista di date più lunga, ordinate
      val finalPair = winner._1
      val finalDates = winner._2.sorted

      // Formato: ((lat1, lon1), (lat2, lon2)) seguita dalle date
      val outputLines = s"(( ${finalPair._1._1}, ${finalPair._1._2} ), ( ${finalPair._2._1}, ${finalPair._2._2} ))" +
        "\n" + finalDates.mkString("\n")

      // Stampa output su console
      println(outputLines)

      // Salva vincitore in un file di testo
      spark.sparkContext.parallelize(Seq(outputLines))
        .coalesce(1)  // Raggruppa su un unico nodo
        .saveAsTextFile(outputPath)
    } else {
      println("Nessuna co-occorrenza trovata.")
    }

    spark.stop()
  }
}

//Vecchio salvataggio
//      //Salva output in un file csv
//      val resultsToSave = results
//        .map(r => (r._1._1.toString, r._1._2.toString, r._2.size, r._2.mkString(",")))
//        .toDF("Loc_A", "Loc_B", "Occorrenze", "Date") // Riscrittura in DataFrame
//      resultsToSave.write
//        .mode("overwrite")
//        .option("header", "true")
//        .csv(outputPath)
//      println(s"\nRisultati salvati correttamente in: $outputPath")
//    } else {
//      println("Nessuna co-occorrenza trovata.")
//    }