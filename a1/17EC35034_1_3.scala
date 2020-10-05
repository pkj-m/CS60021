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



// --- Question 3 --- //

case class Repo(id: String, url: String, owner_id: String, name: String, language: String, created_at: String, forked_from: String, deleted: String, updated_at: String)

// a. Read in the file to an RDD and name it as ‘assignment_2’ and count the number of records

var bufferCSV = sc.textFile("/home/pankaj/sem7/sdm/assignment/important-repos.csv")

// remove the header file from the CSV
var assignment_2 = bufferCSV.mapPartitionsWithIndex { 
    (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}

println("Question 3a: Number of records = ", assignment_2.count()) // output: 1435


// b. How many records in the log file (used in the last 2 questions) refer to entries in the ‘assignment_2’ file ?

def toRec(x: Array[String]): Repo = {
    return Repo(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))
}

def extractRepo(log: Log): String = {
    var message = log.rest.split(Array('/', '?'))
    /* example of message:
    Successful request. URL: https://api.github.com/repos/Particular/NServiceBus.Persistence.ServiceFabric/pulls/10/comments?per_page=100, Remaining: 3333, Total: 110 ms
    
    This can be extracted by splitting the message by the '/' character. In the resulting array, the repo name would be contained in the 5th and 6th elements
    Array(Successful request. URL: https:, "", api.github.com, repos, Particular, NServiceBus.Persistence.ServiceFabric, pulls, 10, comments?per_page=100, Remaining: 3333, Total: 110 ms)
    */
    if (message.length < 6) return "" // in case of a message which does not contain repo name
    if (message(3) != "repos") return "" // in case the message contains user profile and not repo
    var repoName = message(4) + '/' + message(5)
    return repoName
}

var repo_1 = logFiles.keyBy(s => extractRepo(s)).filter(s => s._1 != "").keyBy(s => extractRepo(s._2).split('/')(1))

var records = assignment_2.map(s => s.split(',')).map(s => toRec(s))
var repo_2 = records.keyBy(s => s.name)

val joinRecords = repo_1.join(repo_2)
println("Question 3b: Number of common records in the first and second file = ", joinRecords.count()) // output: Long = 87930

// c. Which of the ‘assignment_2’ repositories has the most failed API calls?

/*

    joinRecords RDD has the following structure: (String, ((String, Log), Repo)) 
    so , we do the following:

    1. filter the records and keep only those which contain Failed access API message. This is done by accessing the s.2.1.2 element which corresponds to the Log element
    2. create a new RDD with key as repo name and value as 1
    3. take sum of all records with common keys
    4. sort by descending order of value
*/
var failedRec = joinRecords.filter(s => s._2._1._2.rest.contains("Failed")).map(s => (s._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
print("Question 3c: Maximum number of faieled API calls made for repository = ",  failedRec.first()._1) // output: (String, Int) = (hello-world,740)