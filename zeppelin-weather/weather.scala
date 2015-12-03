case class WeatherData(
                        date:String,
                        time:String,
                        usaf:String,
                        wban:String,
                        validTemperature:Boolean,
                        temperature:Float,
                        validWindSpeed:Boolean,
                        windSpeed:Float
                      )
def extractWeatherData(row:String) = {
  val date = row.substring(15,23)
  val time = row.substring(23,27)
  val usaf = row.substring(4,10);
  val wban = row.substring(10,15);
  val airTemperatureQuality = row.charAt(92);
  val airTemperature = row.substring(87,92);
  val windSpeedQuality = row.charAt(69);
  val windSpeed = row.substring(65,69);

  WeatherData(date,time,usaf,wban,airTemperatureQuality == '1',airTemperature.toFloat/10,windSpeedQuality == '1',windSpeed.toFloat/10)
}


val rdds = (2009 to 2011) map {i => sc.textFile("/user/cloudera/weather/" + i.toString)}
val weather = sc.union(rdds).map(extractWeatherData(_))
