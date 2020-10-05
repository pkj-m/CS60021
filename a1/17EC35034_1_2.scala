import org.apache.spark.SparkContext    // needed to load RDD
import org.apache.spark.SparkConf
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// --- METHODS COMMON TO ALL QUESTIONS --- //

// let us define a case class called 'Log' with parameters as specified in the question
case class Log(debug_level: String, timestamp: LocalDateTime, download_id: Int, retrieval_stage: String, rest: String)

// converts a text log into a Log object
def toLog(logText: Array[String]): Log = {
    var timestamp = logText(1)
    var datetime = LocalDateTime.parse(timestamp.substring(0, timestamp.length - 6)) // remove the timezone from end
    return Log(logText(0), datetime, logText(2).toInt, logText(3), logText(4))
}

/* An individual log record in the data looks like: 
    DEBUG, 2017-03-24T12:06:23+00:00, ghtorrent-49 -- ghtorrent.rb: Repo Shikanime/print exists

This format can be generalised as follows:
[DEBUG_LEVEL], [TIMESTAMP], ghtorrent-[DOWNLOAD_ID] -- [RETRIEVAL_STAGE]: [REST]

This pattern can be mapped to a regex expression and extracted from the string.
*/

val pattern = """([^\s]+), ([^\s]+), ghtorrent-([^\s]+) -- ([^\s]+): (.*$)""".r // regex pattern matching above pattern

// matches the data with regex pattern and returns the extracted data in an array
// in case the log file is corrupted or doesnt match the pattern, then returns an empty array
def extractData(log: String): Option[Array[String]] = {
    log match {
        case pattern(debug_level, timestamp, download_id, retrieval_stage, rest) => return Some(Array(debug_level, timestamp, download_id, retrieval_stage, rest))
        case default => return None
    }
}

// loads the data from disk to an RDD
def loadRDD(pathToData: String): org.apache.spark.rdd.RDD[Log] = {
    val rawLogs = sc.textFile(pathToData)
    // perform the following sequence of transformations:
    // 1. from each record string, extract array of information, or return None type if record is invalid
    // 2. filter out the None values by flattening the RDD
    // 3. for each valid record array, return the corresponding Log object to make pattern matching easier
    val logFiles = rawLogs.map(s => extractData(s)).flatMap(s => s).map(s => toLog(s))
    return logFiles
}

// --- SETUP ---- //

val pathToData: String = "/home/pankaj/sem7/sdm/assignment/ghtorrent-logs.txt"  // path to data
val logFiles = loadRDD(pathToData)  // load the RDD of Log objects



// --- Question 2 --- //

// a. Create a function that given an RDD and a field (e.g. download_id), it computes an inverted index on the RDD for efficiently searching the records of the RDD using values of the field as keys

def generateInverted(rdd: org.apache.spark.rdd.RDD[Log]): org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Array[Log])])] = {
    val invertRDD = rdd.keyBy(s => s.download_id).map(s => (s._1, Array(s._2))).groupBy(_._1)
    return invertRDD
} 

// b. Compute the number of different repositories accessed by the client ‘ghtorrent-22’ (without using the inverted index).
val reqLog = logFiles.filter(s => s.download_id==22).map(s => extractRepo(s)).distinct.count()
println("Question 2b: Number of different repositories accessed by client ghtorrent-22 = ", reqLog)

// c. Compute the number of different repositories accessed by the client ‘ghtorrent-22’ using the inverted index calculated above.
val invertRDD = generateInverted(logFiles).lookup(22) // this is an array with all client donwload IDs = 22
val repos22 = invertRDD.keyBy(s => extractRepo(s).split('/')(1)).distinct.count()
println("Question 2c: The number of different repos accessed by client ghtorrent-22 = ", repos22)