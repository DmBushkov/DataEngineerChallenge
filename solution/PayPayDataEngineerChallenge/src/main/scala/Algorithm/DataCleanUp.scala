package Algorithm

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataCleanUp {
  // I found several hits made by search systems crawling bots. I decided to remove this requests,
  // because these data is not about users
  val searchBotsUserAgentsRegexps = Seq(
    "aolbuild",
    "baidu",
    "bingbot",
    "bingpreview",
    "msnbot",
    "duckduckgo",
    "adsbot-google",
    "googlebot",
    "mediapartners-google",
    "slurp",
    "yandex",

    "amazon",  // Amazon Route 53 Health Check Service; ref:fb0be1bd-6196-4fd0-b6e0-00a4d4950b40; report http://amzn.to/1vsZADi
    "tweetmemebot",
    "pingdom.com",  // Pingdom.com_bot_version_1.4_(http://www.pingdom.com/)
    "facebook",
    "wordpress.com"  // jetmon/1.0 (Jetpack Site Uptime Monitor by WordPress.com)
  )

  private val userAgentIsNotBot: Column = searchBotsUserAgentsRegexps
    .map(r =>
      not(lower(col("user_agent")).rlike(r))
    )
    .reduce((c1, c2) => c1 and c2)

  // Basic data cleaning.
  def cleanBadHits(df: DataFrame): DataFrame = {
    // Only 27 ips have more than 1000 hits each, but some users have more than 10000, so I decided to remove theese
    // suspiciously active users
    val badUsers: DataFrame = df
      .where(length(col("request")).geq(lit(3000)))  // Some random threshold, probably should be lower
      .select(col("client_ip").as("bad_client_ip"))
      .union(
        df.groupBy("client_ip")
          .agg(sum(lit(1)).as("hits_cnt"))
          .where(col("hits_cnt").geq(lit(1000)))
          .select(col("client_ip").as("bad_client_ip"))
      )
      .distinct()

    df
      .where((col("user_agent").isNotNull) and (col("user_agent") =!= "-"))  // valid users are supposed to use a valid user agent
      .where(userAgentIsNotBot)  // and not to be bots
      .join(badUsers, col("client_ip") === col("bad_client_ip"), joinType = "leftanti")  // and have not very big hits count
      .select(df.columns.map(col): _*)
  }
}
