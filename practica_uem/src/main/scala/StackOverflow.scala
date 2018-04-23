//+++++++ Jesús García García Práctica Infraestructuras - Análisis StackOverflow - Máster Big Data Analytics MBI. +++++++//

import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext


object StackOverflow {

  /** Usage: StackOverflow [file] */
  def main(args: Array[String]) {
	
	//Limpiamos la shell de trazas innecesarias
    Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)
	
	//Si no hay argumentos se sale de la ejecución del programa
    if (args.length < 1) {
      System.err.println("Usage: StackOverflow <file>")
      System.exit(1)
    }
	//Seteamos el nombre de la aplicacion y creamos un nuevo contexto Spark
    val sparkConf = new SparkConf().setAppName("StackOverflow")	
	//Si queremos introducir manualmente las instancias y cores de los ejecutores... 
    //val sparkConf = new SparkConf().setAppName("StackOverflow").set("spark.executor.instances", "15").set("spark.executor.cores", "15")
	
    val sc = new SparkContext(sparkConf)
   
	
	
	//Creamos un nuevo contexto sqlContext.sql
	val sqlContext = new SQLContext(sc)
	//Cargamos por parámetro desde de la shell el fichero hdfs:////stackoverflow/Users.xml
	val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "row").load(args(0))
	//Metemos el contenido en cache para cargar más rápido
	df.cache()
	//Cargamos el contenido de HDFS en una tabla temporal
	df.registerTempTable("tablapracticauem")

	//Imprimir Schema
	
	val columnasBBDD = sqlContext.sql("SELECT * From tablapracticauem")
	columnasBBDD.printSchema()
	
	//++++Inicio Consultas++++//
	
	/*---Si queremos paralelizar consultas 1) Collect de SQL (saca array) 2) repartition y mostrar---*/
	//val cantidadregistros = sqlContext.sql("SELECT count(1) FROM tablapracticauem").collect()
	//val paralelizarcantregRDD = sc.parallelize(cantidadregistros).map(x =>{ x.getLong(0)}).take(1).foreach(println)	
	
	//Mostrar cantidad de registros
	val tinicio = System.currentTimeMillis()
	val cantidadregistros = sqlContext.sql("SELECT _Id FROM tablapracticauem")
	val tiniciorep = System.currentTimeMillis()
	val repartition1 = cantidadregistros.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA1")
	val tfinrep = System.currentTimeMillis()
	val tiniciordd = System.currentTimeMillis()
	val cantidadregistrosRDD = cantidadregistros.rdd.map(x =>{ x.getLong(0)}).take(1).foreach(println)	
	val tfinrdd = System.currentTimeMillis()
	val tfin = System.currentTimeMillis()
	println("Tiempo consulta Mostrar cantidad de registros rep: " + (tfinrep-tiniciorep) + " ms")
	println("Tiempo consulta Mostrar cantidad de registros rdd: " + (tfinrdd-tiniciordd) + " ms")
	println("Tiempo consulta Mostrar cantidad de registros: " + (tfin-tinicio) + " ms")
	
	
	//Mostrar los usuarios con mayor reputación
	val tinicio1 = System.currentTimeMillis()
	val reputacionmayor = sqlContext.sql("SELECT _AccountId,_DisplayName,_Reputation,_UpVotes,_DownVotes From tablapracticauem")
	val tinicio1rep = System.currentTimeMillis()
	val repartition2 = reputacionmayor.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA2")
	val tfin1rep = System.currentTimeMillis()
	val tinicio1rdd = System.currentTimeMillis()
	val reputacionmayorRDD = reputacionmayor.rdd.map(x => { ( x.getString(1) ,x.getLong(2),x.getLong(3),x.getLong(4) ) }).sortBy(- _._2).take(100).foreach(println)
	val tfin1rdd = System.currentTimeMillis()
	val tfin1 = System.currentTimeMillis()
	println("Tiempo consulta Mostrar usuarios mayor reputacion rep: " + (tfin1rep-tinicio1rep) + " ms")
	println("Tiempo consulta Mostrar usuarios mayor reputacion rdd: " + (tfin1rdd-tinicio1rdd) + " ms")
	println("Tiempo consulta Mostrar usuarios mayor reputacion: " + (tfin1-tinicio1) + " ms")
	
	
	//Mostrar los usuarios con menor reputación
	val tinicio2 = System.currentTimeMillis()
	val reputacionmenor = sqlContext.sql("SELECT _AccountId,_DisplayName,_Reputation,_UpVotes,_DownVotes From tablapracticauem")
	val tinicio2rep = System.currentTimeMillis()
	val repartition3 = reputacionmenor.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA3")
	val tfin2rep = System.currentTimeMillis()
	val tinicio2rdd = System.currentTimeMillis()
	val reputacionmenorRDD = reputacionmenor.rdd.map(x => { ( x.getString(1) ,x.getLong(2),x.getLong(3),x.getLong(4) ) }).sortBy(_._2).take(100).foreach(println)
	val tfin2rdd = System.currentTimeMillis()
	val tfin2 = System.currentTimeMillis()
	println("Tiempo consulta Mostrar usuarios menor reputacion rep: " + (tfin2rep-tinicio2rep) + " ms")
	println("Tiempo consulta Mostrar usuarios menor reputacion rdd: " + (tfin2rdd-tinicio2rdd) + " ms")
	println("Tiempo consulta Mostrar usuarios menor reputacion: " + (tfin2-tinicio2) + " ms")
	
	
	//Usuario más antiguo
	val tinicio3 = System.currentTimeMillis()
	val masantiguo = sqlContext.sql("SELECT _AccountId,_DisplayName, _CreationDate From tablapracticauem order by _CreationDate")
	val tinicio3rep = System.currentTimeMillis()
	val repartition4 = masantiguo.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA4")
	val tfin3rep = System.currentTimeMillis()
	val tinicio3rdd = System.currentTimeMillis()
	//Ordena de forma ascendente segun fecha de creacion
	val masantiguoRDD = masantiguo.rdd.map(x => { ((x.getLong(0), x.getString(1),x.getString(2))  ) }).take(100).foreach(println)
	val tfin3rdd = System.currentTimeMillis()
	val tfin3 = System.currentTimeMillis()
	println("Tiempo consulta Mostrar usuarios más antiguos rep: " + (tfin3rep-tinicio3rep) + " ms")
	println("Tiempo consulta Mostrar usuarios más antiguos rdd: " + (tfin3rdd-tinicio3rdd) + " ms")
	println("Tiempo consulta Mostrar usuarios más antiguos: " + (tfin3-tinicio3) + " ms")
	
	
	//Media de acceso o vistas según localización
	val tinicio4 = System.currentTimeMillis()
	val mediaaccesosegunlocalizacion = sqlContext.sql("SELECT _Location, AVG(_Views) as MediaLocalizacion From tablapracticauem group by _Location order by 2 desc")
	val tinicio4rep = System.currentTimeMillis()
	val repartition5 = mediaaccesosegunlocalizacion.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA5")
	val tfin4rep = System.currentTimeMillis()
	val tinicio4rdd = System.currentTimeMillis()
	val mediaaccesosegunlocalizacionRDD = mediaaccesosegunlocalizacion.rdd.map( x => { ( x.getString(0),x.getDouble(1)  ) }).take(100).foreach(println)
	val tfin4rdd = System.currentTimeMillis()
	val tfin4 = System.currentTimeMillis()
	println("Tiempo consulta Media de acceso o vistas según localización rep: " + (tfin4rep-tinicio4rep) + " ms")
	println("Tiempo consulta Media de acceso o vistas según localización rdd: " + (tfin4rdd-tinicio4rdd) + " ms")
	println("Tiempo consulta Media de acceso o vistas según localización: " + (tfin4-tinicio4) + " ms")
	
	
	//Media de edad con más votos (los mejores)
	val tinicio5 = System.currentTimeMillis()
	val mediaedadmasvotos = sqlContext.sql("SELECT _Age, AVG(_UpVotes) as MediaVotos From tablapracticauem where _Age >1 and _Age < 99 group by _Age order by 2 desc")
	val tinicio5rep = System.currentTimeMillis()
	val repartition6 = mediaedadmasvotos.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA6")
	val tfin5rep = System.currentTimeMillis()
	val tinicio5rdd = System.currentTimeMillis()
	val mediaedadmasvotosRDD = mediaedadmasvotos.rdd.map(x => { ( x.getLong(0),x.getDouble(1)  ) }).take(100).foreach(println)
	val tfin5rdd = System.currentTimeMillis()
	val tfin5 = System.currentTimeMillis()
	println("Tiempo consulta Media de edad con más votos rep: " + (tfin5rep-tinicio5rep) + " ms")
	println("Tiempo consulta Media de edad con más votos rdd: " + (tfin5rdd-tinicio5rdd) + " ms")
	println("Tiempo consulta Media de edad con más votos: " + (tfin5-tinicio5) + " ms")
	
	
	//Usuarios con más edad
	val tinicio6 = System.currentTimeMillis()
	val usuariosmasmayores = sqlContext.sql("SELECT _DisplayName,_Age From tablapracticauem order by _Age desc")
	val tinicio6rep = System.currentTimeMillis()
	val repartition7 = usuariosmasmayores.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA7")
	val tfin6rep = System.currentTimeMillis()
	val tinicio6rdd = System.currentTimeMillis()
	val usuariosmasmayoresRDD = usuariosmasmayores.rdd.map(x => { ( x.getString(0),x.getLong(1)  ) }).take(100).foreach(println)
	val tfin6rdd = System.currentTimeMillis()
	val tfin6 = System.currentTimeMillis()
	println("Tiempo consulta Usuarios con mas edad rep: " + (tfin6rep-tinicio6rep) + " ms")
	println("Tiempo consulta Usuarios con mas edad rdd: " + (tfin6rdd-tinicio6rdd) + " ms")
	println("Tiempo consulta Usuarios con mas edad: " + (tfin6-tinicio6) + " ms")
	
	
	//Mostrar usuarios con más votos positivos y que empiecen por cero
	val tinicio7 = System.currentTimeMillis()
	val masvotospositivos = sqlContext.sql("SELECT _DisplayName,_Id,_Age,_Location,_UpVotes From tablapracticauem where _Age >1 and _Age < 99 group by _UpVOtes, _DisplayName, _Id, _Age, _Location Having count(_UpVotes)>0")
	val tinicio7rep = System.currentTimeMillis()
	val repartition8 = masvotospositivos.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA8")
	val tfin7rep = System.currentTimeMillis()
	val tinicio7rdd = System.currentTimeMillis()
	val masvotospositivosRDD = masvotospositivos.rdd.map(x => { ( x.getString(0),x.getLong(1),x.getLong(2),x.getString(3),x.getLong(4)  ) }).sortBy(_._5, ascending=false).take(100).foreach(println)
	val tfin7rdd = System.currentTimeMillis()
	val tfin7 = System.currentTimeMillis()
	println("Tiempo consulta Mostrar usuarios con más votos positivos y que empiecen por cero rep: " + (tfin7rep-tinicio7rep) + " ms")
	println("Tiempo consulta Mostrar usuarios con más votos positivos y que empiecen por cero rdd: " + (tfin7rdd-tinicio7rdd) + " ms")
	println("Tiempo consulta Mostrar usuarios con más votos positivos y que empiecen por cero: " + (tfin7-tinicio7) + " ms")
	
	
	//Contar sitios de España
	val tinicio8 = System.currentTimeMillis()
	val contarsitiosespania = sqlContext.sql("SELECT _location, COUNT(1) From tablapracticauem where upper(_location) like '%ESPAÑA%' group by _location order by 2 desc")
	val tinicio8rep = System.currentTimeMillis()
	val repartition9 = contarsitiosespania.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA9")
	val tfin8rep = System.currentTimeMillis()
	val tinicio8rdd = System.currentTimeMillis()
	val contarsitiosespaniaRDD = contarsitiosespania.rdd.map(x => { (( x.getString(0),x.getLong(1))  ) }).take(100).foreach(println)
	val tfin8rdd = System.currentTimeMillis()
	val tfin8 = System.currentTimeMillis()
	println("Tiempo consulta Contar sitios de España rep: " + (tfin8rep-tinicio8rep) + " ms")
	println("Tiempo consulta Contar sitios de España rdd: " + (tfin8rdd-tinicio8rdd) + " ms")
	println("Tiempo consulta Contar sitios de España: " + (tfin8-tinicio8) + " ms")
	
	
	//Media reputación por país
	val tinicio9 = System.currentTimeMillis()
	val reputacionpais = sqlContext.sql("SELECT _location,sum,count,sum/count from (select _location, sum(_reputation) sum, count(1) count from tablapracticauem group by _location order by 2 desc) ")
	val tinicio9rep = System.currentTimeMillis()
	val repartition10 = reputacionpais.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA10")
	val tfin9rep = System.currentTimeMillis()
	val tinicio9rdd = System.currentTimeMillis()
	val reputacionpaisRDD = reputacionpais.rdd.map(x => { ( x.getString(0),x.getLong(1),x.getLong(2),x.getDouble(3)  ) }).take(100).foreach(println)
	val tfin9rdd = System.currentTimeMillis()
	val tfin9 = System.currentTimeMillis()
	println("Tiempo consulta Media reputacion por pais rep: " + (tfin9rep-tinicio9rep) + " ms")
	println("Tiempo consulta Media reputacion por pais rdd: " + (tfin9rdd-tinicio9rdd) + " ms")
	println("Tiempo consulta Media reputacion por pais: " + (tfin9-tinicio9) + " ms")
	
	
	//Usuarios que hayan tenido más vistas
	val tinicio10 = System.currentTimeMillis()
	val usuariosvistas = sqlContext.sql("SELECT _DisplayName, _AccountId,_Id,_LastAccessDate,_Views from tablapracticauem group by _Views, _DisplayName, _AccountId,_Id,_LastAccessDate Having count(_Views)>=0 order by _Views desc")
	val tinicio10rep = System.currentTimeMillis()
	val repartition11 = usuariosvistas.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA11")
	val tfin10rep = System.currentTimeMillis()
	val tinicio10rdd = System.currentTimeMillis()
	val usuariosvistasRDD = usuariosvistas.rdd.map(x => { ( x.getString(0) ,x.getLong(1),x.getLong(2),x.getString(3),x.getLong(4) ) }).take(100).foreach(println)
	val tfin10rdd = System.currentTimeMillis()
	val tfin10 = System.currentTimeMillis()
	println("Tiempo consulta Usuarios que hayan tenido mas vistas rep: " + (tfin10rep-tinicio10rep) + " ms")
	println("Tiempo consulta Usuarios que hayan tenido mas vistas rdd: " + (tfin10rdd-tinicio10rdd) + " ms")
	println("Tiempo consulta Usuarios que hayan tenido mas vistas: " + (tfin10-tinicio10) + " ms")
	
	
	//Mostrar la fecha de creación de aquellas cuentas que tengan más de 4000 votos positivos
	val tinicio11 = System.currentTimeMillis()
	val votospositivos = sqlContext.sql("SELECT _CreationDate, _UpVotes From tablapracticauem where _UpVotes > 40000")
	val tinicio11rep = System.currentTimeMillis()
	val repartition12 = votospositivos.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA12")
	val tfin11rep = System.currentTimeMillis()
	val tinicio11rdd = System.currentTimeMillis()
	val votospositivosRDD = votospositivos.rdd.map(x => { ( x.getString(0) ,x.getLong(1) ) }).sortBy(_._2).take(100).foreach(println)
	val tfin11rdd = System.currentTimeMillis()
	val tfin11 = System.currentTimeMillis()
	println("Tiempo consulta Cuentas que hayan tenido mas de 4000 votos positivos rep: " + (tfin11rep-tinicio11rep) + " ms")
	println("Tiempo consulta Cuentas que hayan tenido mas de 4000 votos positivos rdd: " + (tfin11rdd-tinicio11rdd) + " ms")
	println("Tiempo consulta Cuentas que hayan tenido mas de 4000 votos positivos: " + (tfin11-tinicio11) + " ms")
	
	
	//Mostrar el maximo de edad de los usuarios de StackOverFlow comprendida entre 0 y 65 años
	val tinicio12 = System.currentTimeMillis()
	val maximoedad = sqlContext.sql("SELECT _Age, COUNT(*) From tablapracticauem WHERE _Age is not null AND (_Age > 0) AND (_Age < 65) GROUP BY _Age" )
    val tinicio12rep = System.currentTimeMillis()
	val repartition13 = maximoedad.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA13")
	val tfin12rep = System.currentTimeMillis()
	val tinicio12rdd = System.currentTimeMillis()
	val maximoedadRDD = maximoedad.rdd.map(x => { (x.getLong(0), x.getLong(1) ) }).take(100).foreach(println)
	val tfin12rdd = System.currentTimeMillis()
	val tfin12 = System.currentTimeMillis()
	println("Tiempo consulta Mostrar el maximo de edad de los usuarios de StackOverFlow comprendida entre 0 y 65 años rep: " + (tfin12rep-tinicio12rep) + " ms")
	println("Tiempo consulta Mostrar el maximo de edad de los usuarios de StackOverFlow comprendida entre 0 y 65 años rdd: " + (tfin12rdd-tinicio12rdd) + " ms")
	println("Tiempo consulta Mostrar el maximo de edad de los usuarios de StackOverFlow comprendida entre 0 y 65 años: " + (tfin12-tinicio12) + " ms")
	
	
	//Mostrar diferencia entre votos positivos y negativos
	val tinicio13 = System.currentTimeMillis()
	val diferenciavotos = sqlContext.sql(" SELECT _AccountId, _DisplayName , _UpVotes - _DownVotes AS Relacion From tablapracticauem ")
    val tinicio13rep = System.currentTimeMillis()
	val repartition14 = diferenciavotos.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("PRUEBA14")
	val tfin13rep = System.currentTimeMillis()
	val tinicio13rdd = System.currentTimeMillis()
	val diferenciavotosRDD = diferenciavotos.rdd.map(x => { ( x.getLong(0), x.getString(1) ,x.getLong(2) ) }).take(100).foreach(println)
	val tfin13rdd = System.currentTimeMillis()
	val tfin13 = System.currentTimeMillis()
	println("Tiempo consulta Mostrar diferencia entre votos positivos y negativos rep: " + (tfin13rep-tinicio13rep) + " ms")
	println("Tiempo consulta Mostrar diferencia entre votos positivos y negativos rdd: " + (tfin13rdd-tinicio13rdd) + " ms")
	println("Tiempo consulta Mostrar diferencia entre votos positivos y negativos: " + (tfin13-tinicio13) + " ms")
		
	//++++Fin consultas++++//
	
	
	
	
	
	
    sc.stop()
  }
}

