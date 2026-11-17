import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// ============================================================
// QUESTION 1 : Construction du Graphe avec Spark GraphX
// ============================================================

// Chaque sommet contient : (Nom de la ville, Pays)
val verticesArray = Array(
  (1L,  ("Atlas",  "Maroc")),
  (2L,  ("Rif",    "Maroc")),
  (3L,  ("Souss",  "Maroc")),
  (4L,  ("Tanger", "Maroc")),
  (5L,  ("Oujda",  "Maroc")),
  (6L,  ("Fes",    "Maroc")),
  (7L,  ("Lyon",   "France")),
  (8L,  ("Brest",  "France")),
  (9L,  ("Nice",   "France")),
  (10L, ("Zug",    "Suisse")),
  (11L, ("Bale",   "Suisse")),
  (12L, ("Colon",  "Panama")),
  (13L, ("Balboa", "Panama"))
)

// Les 21 aretes orientees ponderees par le montant (en k€)
val edgesArray = Array(
  // Reseau local Maroc
  Edge(1L, 2L, 10.0),  // Atlas -> Rif
  Edge(1L, 3L, 20.0),  // Atlas -> Souss
  Edge(3L, 2L, 15.0),  // Souss -> Rif
  Edge(4L, 5L, 8.0),   // Tanger -> Oujda
  Edge(4L, 6L, 5.0),   // Tanger -> Fes
  Edge(6L, 5L, 6.0),   // Fes -> Oujda

  // Maroc -> France
  Edge(1L, 7L, 40.0),  // Atlas -> Lyon
  Edge(2L, 7L, 25.0),  // Rif -> Lyon
  Edge(3L, 9L, 30.0),  // Souss -> Nice

  // Interne France (Cycle francais)
  Edge(7L, 9L, 12.0),  // Lyon -> Nice
  Edge(9L, 8L, 18.0),  // Nice -> Brest
  Edge(8L, 7L, 9.0),   // Brest -> Lyon

  // France -> Suisse
  Edge(7L, 10L, 60.0), // Lyon -> Zug
  Edge(8L, 10L, 45.0), // Brest -> Zug
  Edge(8L, 11L, 35.0), // Brest -> Bale
  Edge(9L, 11L, 55.0), // Nice -> Bale

  // Interne Suisse
  Edge(10L, 11L, 30.0), // Zug -> Bale

  // Suisse -> Panama
  Edge(10L, 12L, 80.0), // Zug -> Colon
  Edge(11L, 13L, 50.0), // Bale -> Balboa

  // Interne Panama
  Edge(12L, 13L, 90.0), // Colon -> Balboa

  // Panama -> Suisse (Cycle international)
  Edge(12L, 10L, 70.0)  // Colon -> Zug
)

val vRDD: RDD[(VertexId, (String, String))] = sc.parallelize(verticesArray)
val eRDD: RDD[Edge[Double]] = sc.parallelize(edgesArray)

val bankGraph: Graph[(String, String), Double] = Graph(vRDD, eRDD)




// ============================================================
// QUESTION 2 : Villes les plus connectees
// ============================================================
println("\n=== QUESTION 2 : VILLES LES PLUS CONNECTEES ===")

val totalDeg = bankGraph.degrees
val degWithNames = bankGraph.vertices.leftJoin(totalDeg) {
  case (id, (name, country), optDeg) => (name, optDeg.getOrElse(0))
}.sortBy(_._2._2, ascending = false)

degWithNames.collect.foreach { case (id, (name, deg)) =>
  println(f"Ville : $name%-8s | Connexions : $deg")
}





// ============================================================
// QUESTION 3 : Solde net d'argent (recu - envoye)
// ============================================================
println("\n=== QUESTION 3 : SOLDE NET D'ARGENT POUR CHAQUE VILLE ===")

// Montants recus par chaque sommet (arrivees a destination)
val inMoney = bankGraph.aggregateMessages[Double](
  triplet => triplet.sendToDst(triplet.attr),
  (a, b) => a + b
)

// Montants envoyes par chaque sommet (departs de source)
val outMoney = bankGraph.aggregateMessages[Double](
  triplet => triplet.sendToSrc(triplet.attr),
  (a, b) => a + b
)

// Calcul : Recu - Envoye
val soldeGraph = bankGraph.outerJoinVertices(inMoney) {
  case (id, (name, country), optIn) => (name, optIn.getOrElse(0.0))
}.outerJoinVertices(outMoney) {
  case (id, (name, inM), optOut) => (name, inM - optOut.getOrElse(0.0))
}

soldeGraph.vertices.sortBy(_._1).collect.foreach { case (id, (name, solde)) =>
  println(f"Ville : $name%-8s | Solde net : $solde%+6.1f k€")
}





// ============================================================
// QUESTION 4 : Cycles / L'argent tourne en boucle
// ============================================================
println("\n=== QUESTION 4 : CYCLES DE VIREMENTS EN BOUCLE ===")

// Calcul des Composantes Fortement Connexes (SCC)
val scc = bankGraph.stronglyConnectedComponents(numIter = 20)

bankGraph.vertices.join(scc.vertices).collect.groupBy(_._2._2).filter(_._2.size > 1).foreach {
  case (compId, members) =>
    val villes = members.map(_._2._1._1).mkString(", ")
    val pays = members.head._2._1._2
    println(s"-> Cycle identifie : [ $villes ] (Region / Pays : $pays)")
}






// ============================================================
// QUESTION 5 : Flux circulant entre pays
// ============================================================
println("\n=== QUESTION 5 : FLUX ENTRE PAYS ===")

// On ne retient que les trajets dont le pays source != pays destination
val interCountryFlows = bankGraph.triplets
  .filter(t => t.srcAttr._2 != t.dstAttr._2)
  .map(t => ((t.srcAttr._2, t.dstAttr._2), t.attr))
  .reduceByKey(_ + _)

interCountryFlows.collect.sortBy(-_._2).foreach { case ((srcCountry, dstCountry), total) =>
  println(f"$srcCountry%-8s -> $dstCountry%-8s : $total%.0f k€")
}






// ============================================================
// QUESTION 6 : Virements internes par pays
// ============================================================
println("\n=== QUESTION 6 : VIREMENTS INTERNES PAR PAYS ===")

// On ne retient que les trajets dont le pays source == pays destination
val intraCountryFlows = bankGraph.triplets
  .filter(t => t.srcAttr._2 == t.dstAttr._2)
  .map(t => (t.srcAttr._2, t.attr))
  .reduceByKey(_ + _)

intraCountryFlows.collect.sortBy(-_._2).foreach { case (country, total) =>
  println(f"Virements internes $country%-8s : $total%.0f k€")
}