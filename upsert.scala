import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.Properties
import java.sql._


object compileDF {

   def updateOracleDB(spark: SparkSession, inputDF: DataFrame, connectnPro: Properties, tableName: String, primaryKey : String, partkey: String, rowMap :Map[String,String]) {
     import spark.implicits._
     inputDF.foreachPartition(partition => {
       val jdbcUrl = connectnPro.getProperty("jdbcUrl")
       val userid = connectnPro.getProperty("user")
       val password = connectnPro.getProperty("password")
        val dbConnection = DriverManager.getConnection(jdbcUrl, userid ,password)
       var st:Statement  = dbConnection.createStatement()
        val batchSize = 10000
       partition.grouped(batchSize).foreach(batch=> {

         st.clearBatch()
         batch.foreach { row =>
           //println("Processing row")
           var updateRequired : Boolean = false
           var actionRequired : Boolean = false
           var primaryKeyIndex : Int = 0
           var primarKeyData : String = ""
           var partKeyIndex : Int = 0
           var partKeyData : String = ""
             primaryKeyIndex  = row.fieldIndex(primaryKey)
             primarKeyData = row.getString(primaryKeyIndex)
             partKeyIndex  = row.fieldIndex(partkey)
             partKeyData = row.getTimestamp(partKeyIndex).toString
             updateRequired = true
             val sqlQry = "Select * from " + tableName + " where " + primaryKey + "= '" + primarKeyData + "'"
             //println("Checking row:" + sqlQry)
             val stmt = dbConnection.createStatement()
             val rsltSet = stmt.executeQuery(sqlQry)
             val count =
               if (rsltSet.next()) { 1 }
               else { 0 }
               if (count == 0)
                   updateRequired = false
           rsltSet.close()
           stmt.close()
           //}
           var sqlQuery :String = ""

           if (updateRequired)
           {
             println("In update")

             //println("primarKeyData:"+ primarKeyData)
             sqlQuery = "Update "+ tableName + " Set "
             for ((key,value)<-rowMap)
             {
                 if (value == "Int")
                 {
                   val inputValue: Int= row.getAs[Int](key)
                   if (inputValue == null)
                   {
                     sqlQuery += key + "=null"
                   }
                   else
                   {

                   sqlQuery += key + "= " + inputValue + " " }

                 }
                 else if (value == "Long")
                 {
                   val inputValue: Long= row.getAs[Long](key)
                   if (inputValue == null)
                   {
                     sqlQuery += key + "=null"
                   }
                   else
                   {

                   sqlQuery += key + "= " + inputValue + " " }

                 }
                 else if (value == "String")
                 {
                   val inputValue = row.getAs[String](key)
                   if (inputValue == null)
                   {
                     sqlQuery += key + "=null"
                   }
                   else
                   {

                   sqlQuery += key + "= '" + inputValue + "' " }
                 }
                 else if (value == "Double")
                 {
                   val inputValue = row.getAs[Double](key)
                   if (inputValue == null)
                   {
                     sqlQuery += key + "=null"
                   }
                   else
                   {

                   sqlQuery += key + "= " + inputValue.toString + " " }
                 }
                 else if (value == "Timestamp")
                 {
                   val inputValue = row.getAs[java.sql.Timestamp](key)
                   if (inputValue == null)
                   {
                     sqlQuery += key + "=null"
                   }
                   else
                   {
                     var timestampStr = inputValue.toString.split('.')(0)
                     timestampStr = "to_timestamp('" + timestampStr +"', 'YYYY-MM-DD HH24:MI:SS')"
                     sqlQuery += key + "= " + timestampStr + " "
                   }
                 }
                 else if (value == "Date")
                 {
                   val inputValue = row.getAs[java.sql.Date](key)
                   if (inputValue == null)
                   {
                     sqlQuery += key + "=null"
                   }
                   else
                   {

                     sqlQuery += key + "= " + "to_date('" + inputValue.toString + "', 'yyyy-mm-dd')" + " "
                   }
                 }
                 else
                   null
                 if (rowMap.last._1 != key)
                   sqlQuery += ","
             }

             sqlQuery += "where " + primaryKey + " = '" + primarKeyData + "'"
           }
           /* else /////////uncomment for insert functionality
           {

             //println("In insert:" + row)
             sqlQuery = "Insert into " + tableName + " ("
             val keysItr = rowMap.keys.iterator
             var first = true
             while (keysItr.hasNext)
             {
               if (first)
                 first = false
               else
                 sqlQuery += ","
               sqlQuery += keysItr.next()
             }
             sqlQuery += ") values ("
             first = true
             for ((key,value)<-rowMap)
             {
                 if (first)
                   first = false
                 else
                   sqlQuery += ","
                 if (value == "Int")
                 {
                   val inputValue: Int= row.getAs[Int](key)
                   if (inputValue == null)
                   {
                     sqlQuery += "null"
                   }
                   else
                   {

                   sqlQuery += inputValue}

                 }
                 else if (value == "Long")
                 {
                   val inputValue: Long= row.getAs[Long](key)
                   if (inputValue == null)
                   {
                     sqlQuery += "null"
                   }
                   else
                   {

                   sqlQuery += inputValue}

                 }
                 else if (value == "String")
                 {
                   val inputValue = row.getAs[String](key)
                   if (inputValue == null)
                   {
                     sqlQuery += "null"
                   }
                   else
                   {

                   sqlQuery += "'" + inputValue + "'" }
                 }
                 else if (value == "Double")
                 {
                   val inputValue = row.getAs[Double](key)
                   if (inputValue == null)
                   {
                     sqlQuery += "null"
                   }
                   else
                   {

                   sqlQuery += inputValue }
                 }
                 else if (value == "Timestamp")
                 {
                   val inputValue = row.getAs[java.sql.Timestamp](key)
                   if (inputValue == null)
                   {
                     sqlQuery +=  "null"
                   }
                   else
                   {
                     var timestampStr = inputValue.toString.split('.')(0)
                     timestampStr = "to_timestamp('" + timestampStr +"', 'YYYY-MM-DD HH24:MI:SS')"
                     sqlQuery += timestampStr
                   }
                 }
                 else if (value == "Date")
                 {
                   val inputValue = row.getAs[java.sql.Date](key)
                   if (inputValue == null)
                     sqlQuery +=  "null"
                   else
                   {

                     sqlQuery += "to_date('" + inputValue.toString + "', 'yyyy-mm-dd')"
                   }
                 }
                 else
                   null
             }
             sqlQuery += ")"
           } */
           //println("Query:" + sqlQuery)
           //println("Adding query to batch")
           st.addBatch(sqlQuery)
           //println("added to batch")

           }

           println ("Executing Batch")
                    try {st.executeBatch()
                        dbConnection.commit() } catch { case ex: Exception => { println("EmptyBatch") }}
           println ("Batch Commited")
       } )
       st.close()
       dbConnection.close()

     })

   }
}

