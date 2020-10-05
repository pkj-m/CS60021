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

// --- Question 1 --- //

// a. How many lines does the RDD contain?

println("Question 1a: Number of lines in RDD = ", logFiles.count()) // Output: Long = 9669634 

// b. Count the number of "WARN" messages?

def countWARN(log: Log): Int = {
    if (log.debug_level == "WARN") return 1
    return 0
}

val warnLogsCount = logFiles.map(s => countWARN(s)).reduce((a, b) => a + b) //for each log record, store 1 if the record is a WARN record, else 0. Finally return sum of values of all records
println("Question 1b: Number of WARN messages = ", warnLogsCount) // Output: Int = 132158

// c. How many repos were processed in total when the retrieval_stage is "api_cleint"?

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

val logs_repo = logFiles.filter(s => s.retrieval_stage=="api_client.rb").map(s => extractRepo(s))
val repoCount = logs_repo.distinct.count()
println("Question 1c: Number of repositories processed with api_client as retrieval_stage = ", repoCount) // 71981


// d. Using retrieval_stage as “api_client”, find which clients did the most HTTP requests and FAILED HTTP requests from the download_id field

/*
    the answer is obtained by performing the following operations:

    1. filter out records which are not api_client requests
    2. create key-value pair by download_id
    3. set the value of each pair to 1
    4. reduce the pairs by adding records with common keys
    5. sort in descending order of value (record numbers)

    for the second part, add additional condition in filter that the rest paramter should start with 'failed'
*/
val httpRequests = logFiles.filter(s => s.retrieval_stage=="api_client.rb").keyBy(s => s.download_id).map(s => (s._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
val maxhttpRequest = httpRequests.first()
println("Question 1d: Client with maximum HTTP requests = ", maxhttpRequest._1)   // download_id with maximum number of HTTP requests

val httpFailedRequests = logFiles.filter(s => s.retrieval_stage=="api_client.rb" && s.rest.substring(0, 6) == "Failed").keyBy(s => s.download_id).map(s => (s._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
val maxhttpFailedRequest = httpFailedRequests.first()
println("Question 1d: Client with maximum number of failed HTTP requests = ", maxhttpFailedRequest)

// e. Find the most active hour of the day and most active repository

/*
    the answer for most active hour can be obtained in a similar manner as previous question, except the key for each pair will be the hour in the timestamp of the request
    the answer for the second part can be done in exactly the same manner except the RDD used will be not the logs RDD but the repo RDD generated in the previous parts
*/

val logHours = logFiles.keyBy(s => s.timestamp.getHour).map(s => (s._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
val activeHour = logHours.first()
println("Question 1e: The most active hour of the day = ", activeHour._1)

val repoLogs = logs_repo.filter(s => s != "").keyBy(s => s).map(s => (s._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
val activeRepo = repoLogs.first()
println("Question 1e: The most active repository = ", activeRepo._1)

// f. Which access keys are failing most often?

/*
The logs containing the access keys look like this:
Log(WARN,2017-03-23T20:04:28,13,api_client.rb,Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 3031)

Therefore, we need to filter out the Failed requests record, and then find the ones which contain the keyword 'Access'
*/

val failedAccessRecords = logFiles.filter(s => (s.rest.substring(0, 6) == "Failed" && s.rest.contains("Access"))).map(s => s.rest).map(s => s.split(',')).map(s => (s.filter(y => y.contains("Access")))(0)).map(s => s.split(' ')).map(s => s(s.length - 1))
val failedCount = failedAccessRecords.keyBy(s => s).map(s => (s._1, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
println(f"Question 1f: The access key which fails the most is = ", failedCount.first()._1)




