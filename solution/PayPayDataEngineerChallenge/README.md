# PayPayDataEngineerChallenge Solution by Dmitrii Bushkov

## Usage


### How to build
I used SBT plugin "Assembly" to pack everything except Spark into one jar.
```
sbt assembly
```

### Running
```
Usage: Solution for PayPay's Data Engineer Challenge: https://github.com/Pay-Baymax/DataEngineerChallenge [options]

  -i, --input_hits <value>
                           Input path to hits log. Use `file:///path/to/data` if file is local.
  -s, --input_schema <value>
                           Path to a schema for input hits data. Should be an existing valid local YAML-file.
  --idle_time <value>      Time period that separates two adjacent sessions. If time between two adjacent hits is bigger than this, second hit will be assigned to a new session.
  -m, --method <value>     Method for a session building algorithm: window functions, custom aggregating function or graph connected components.
  -o, --output_result <value>
                           Output path for processing result. Use `file:///path/to/data` if file is local. If not passed, results would be shown in log with `.show()` method.
  --graph_iters <value>    Iterations quantity for graph method.
```


Launch command example: 
```(sh)
spark-submit --class Main.Main  --conf spark.driver.memory=8g --conf spark.default.parallelism=16 --conf spark.sql.shuffle.partitions=16 --master local[4] PayPayDataEngineerChallenge-assembly-0.1.jar -i file:///path/to/input/2015_07_22_mktplace_shop_web_log_sample.log.gz -s ./unaggregated_schema.yaml -o file:///path/to/output/DataEngineerChallenge/output -m window
```



## Structure

### Main
[Main](./src/main/scala/Main/Main.scala) is an entry point of this Spark app. Methods to read/write data, perform some initial data cleaning and aggregate sessions are called there along with data transformations to prepare raw data and count statistics on prepared sessions.

### Utils
[Utils](./src/main/scala/Utils) package contains objects for command line arguments parsing and read/write Spark operations. 

###
[Algorithm](./src/main/scala/Algorithm) package contains one object for data cleaning and three objects for different session aggregators.


### data
[data](./data) contains an input log file and auxiliary data like YAML-file describing input data schema.



## Solution details

### Data cleaning

Data clinig is performed by [DataCleanUp](./src/main/scala/Algorithm/DataCleanUp.scala) object. 


There are two steps:
1. Handle user agents. Lots of hits are logged with user agents of search robots or some bots. Robots make lots of hits, but these sessions should be analyzed in a different way since the goals of robots and ordinary users are different. Also I supposed that every hit mast have a filled user agent, so all hits with empty of robotic user agent were removed.
2. Remove users with lots of hits or strange request. Less than 30 users made more than 1000 hits each. The maximum amount of hits is greater than 40000. I decided to remove all hits of these users: this behavior is suspisious, an ordinary user can't make this much hits. Also, some users make request to suspisiously long URLs, and some users do both of these strange actions.


These cleaning steps are quite primitive. Robots detection should be performed as soon as possible after the hit happens, and must be based on many features. Ideally, data from services should already be logged with a markup for robots and not. A similar situation occurs with fraud. I don't know if it's common for this system to have users with thousands hits. For example, our product might be a shop or a classified and these users could probably be some business partners, who can use API to place thousands of goods to our service. And there might be some valid logic behind requesting long seemingly meaningless links. Fraud detection must also be performed separately, so fraud users markup would be accessible and possible to use for analytics. In real world I would read the documentation or consult with developers responsible for the log before filtering, but for this problem I decided to filter suspisious users. Also some filterings could be based on HTTP-statuses or probably SSL ciphers, but I decided not to use it for now.



Also after I built sessions, I saw examples of weird behavior like hitting 2 URLs for 300 times in one session, visiting new URL every 5 seconds etc. This information could also be useful for detecting of unwanted interaction with our service.




### Data transformations

Initial transformations are made in Main object. `event_time_utc` field looks valid, so I extracted microseconds since epoch from this timestamp for convenience when calculating the time difference between hits (and probably Long number is more lightweighted than Spart TimestampType). `client_ip_port` is also always valid according to data. I separated IP and port and used only IP as user identifier. As I could see, requests from same IP with different ports were made from one user because of same user agent and time between hits. Also I know browsers use several local ports. So I concidered port to be useless for clustering. Also I extracted URL from `request` field and UserAgent (and had some problems with fields shifting in the log).




### Sessions building
I chose `client_ip` and `user_agent` as keys for clustering, because if user changed his user agent, then he probably started a new session. Difference between session is passed via command line argument `idle_time` in minutes. Algorithm is very simple: if there are more than `idle_time` minutes between two adjacent hits, the lates one belongs to a new session. The default for `idle_time` is 30 minutes, because as I know, Yandex Metrica uses same amount of time. Also we could use some UTM-marks in request or referrers (if we had this data) or force sessions to end at midnight (like Google Analytics does), but I decided not to do this.




### Session building algorithms itselves
I was immediately able to come up with three options for how to collect sessions, the description below is ordered from the worst to the best.
1. Spark Graph API and looking for connected components in [GraphSessionBuilder](./src/main/scala/Algorithm/GraphSessionBuilder.scala). Let hits be vertices and two vertices are connected by edge if these hits was made by the same user with same user agent and time between hits is less than `idle_time`-argument. Then every session is a just connected component in this graph. 
Although I have experience with successfully applying such a method (for example, for clustering airline passengers), this approach is way to complicated for this problem, its code is heavy, it requires lots of iterations to converge and partial results are indeterministic. Also lots of heavy actions are required to prepare edges (trade-off between heavy data in vertices or complex edges creating). Session ID is a global row number of all hits.
2. Custom User Defined Aggregate Function in [CustomSessionUDAF](./src/main/scala/Algorithm/CustomSessionUDAF.scala). Works much better, results are deterministic, but also a little bit to complicated: this function is highly specialized, probably not going to be reused, and must be optimized by an author, while first and second methods can be optimized by Spark internal methods. Returns same results as third method. Session ID is tuple (ip, user agent, first hit of session timestamp), unique for every (user, user agent) pair.
3. Just window functions in [WindowFunctionSessionBuilder](./src/main/scala/Algorithm/WindowFunctionSessionBuilder.scala). Lets use LAG() to immediately find every first hit of every session (and nulls at rows between). Lets use MAX() on the result (between first and current rows) to fill gaps. Session ID is tuple (ip, user agent, first hit of session timestamp), unique for every (user, user agent) pair. Works pretty fast and code is short. Can be rewritten in SQL easily. Result is deterministic.


### Further aggregating
The final task is ambiguous, so I made it as I understood. I made global aggregation of `avg_session_duration_sec` and `avg_urls_visited` per session withou any other dimensions. Also I aggregated data per each session and computed duration and unique URLs quantity and ordered this DataFrame by descending of session duration. So we have URLs count and session duration in one table and can take any users/session according to goals: top N by duration, first N% by duration, n-tiles etc.


If `-o/--output_result` argument was passed, result data will be stored in subdirectories, otherwise just shown.



