package com.thomsonreuters.ema.example.scala
import com.thomsonreuters.ema.access.OmmReal.MagnitudeType
import com.thomsonreuters.ema.access.EmaFactory
import com.thomsonreuters.ema.access.FieldList 
import com.thomsonreuters.ema.access.FieldEntry
import com.thomsonreuters.ema.access.Map
import com.thomsonreuters.ema.access.MapEntry
import java.util.Calendar
import java.nio.ByteBuffer;
import java.util.TimeZone
import scala.util.Random

//This class is to update data, create Market Maker's map containing data 
//which will be added in the payload of refresh or update messages
class MarketMaker {
  //SummaryData also contains field id 1709 with fixed value
  //field id 1 PROD_PERM(PERMISSION) will be added in summary data if refreshPE > 0
  private var activeDate: Calendar = null //field id 17
  private var timActMs: Long = 75610613 //field id 4148
  
  //MapEntry data
  private var bid: Int = 4115 //field id 22, 2 decimals
  private var ask: Int = 4282 //field id 25, 2 decimals
  private var bidSize: Int = 100 //field id 30
  private var askSize: Int = 110 //field id 31
  private var askTimMs: Long = 43459027 //field id 4147
  private var bidTimMs: Long = 42911172 //field id 4150
  
  //the prefix for an entryKey
  private val prefix: String = "ARIC_"
  private var entryKey: String = ""
  
  //generate an entryKey for a map entry
  def generateEntryKey(): String = {
    //random 3 characters from A to Z 
    val subfix: Array[Char] = Array( (Random.nextInt(26) + 65).toChar , (Random.nextInt(26) + 65).toChar,  (Random.nextInt(26) + 65).toChar )
    //Example entryKey:ARIC_KRR, ARIC_VXQ
    return(prefix + subfix.mkString)
 }

  //The method sets new value of each field in a refresh and an update message
  def updateData() {
    bid = bid + 211;
    ask = ask + 221;
    bidSize = bidSize + 10
    askSize= askSize + 15
    bidTimMs = bidTimMs + 1000
    askTimMs = askTimMs + 1000
  }
  
  //The method returns Market Maker data contained in the payload of a refresh message
  def generateMMRefresh(refreshPE: Long, update: Boolean) : Map =   {
    //updateData() is invoked after fail over is successful, 
    //then creates the payload of a refresh message with new values
    //otherwise, use the initialized values
    if(update)
        updateData()
    
    //Create a sumary data
    val mapSummaryData: FieldList  = EmaFactory.createFieldList();
    if(refreshPE > 0) {
        mapSummaryData.add( EmaFactory.createFieldEntry().uintValue(1, refreshPE))
     }
    activeDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    mapSummaryData.add( EmaFactory.createFieldEntry().date(17, 
                  //month is zero based so + 1
                  activeDate.get(Calendar.YEAR), activeDate.get(Calendar.MONTH) +1, activeDate.get(Calendar.DATE)))
		mapSummaryData.add(EmaFactory.createFieldEntry().enumValue(1709,  320));
		mapSummaryData.add(EmaFactory.createFieldEntry().uintValue(4148,  timActMs));
		
		//Create a map containing 5 map entries
		val map: Map = EmaFactory.createMap()
		map.summaryData(mapSummaryData)
	  for( a <- 1 to 5) { 
		  val entryData: FieldList = EmaFactory.createFieldList()
		  entryData.add( EmaFactory.createFieldEntry().real(22, bid, MagnitudeType.EXPONENT_NEG_2))
      entryData.add( EmaFactory.createFieldEntry().real(25, ask, MagnitudeType.EXPONENT_NEG_2))
      entryData.add( EmaFactory.createFieldEntry().real(30, bidSize,MagnitudeType.EXPONENT_0))
      entryData.add( EmaFactory.createFieldEntry().real(31, askSize, MagnitudeType.EXPONENT_0))
      entryData.add(EmaFactory.createFieldEntry().uintValue(4147,askTimMs))
      entryData.add(EmaFactory.createFieldEntry().uintValue(4150,bidTimMs))
      entryKey = generateEntryKey()
  		map.add(EmaFactory.createMapEntry().keyAscii(entryKey, MapEntry.MapAction.ADD, entryData));
  	  if (a < 5)
			  updateData();
		}
    return map 
   }
  
  //The method returns Market Maker data contained in the payload of an update message
  def generateMMUpdate() : Map =   {
    //Set new value to each field
    updateData();
    
    //Create a map containing 1 map entry
		val map: Map = EmaFactory.createMap();
		val entryData: FieldList = EmaFactory.createFieldList()
		entryData.add( EmaFactory.createFieldEntry().real(22, bid, MagnitudeType.EXPONENT_NEG_2))
    entryData.add( EmaFactory.createFieldEntry().real(25, ask, MagnitudeType.EXPONENT_NEG_2))
    entryData.add( EmaFactory.createFieldEntry().real(30, bidSize,MagnitudeType.EXPONENT_0))
    entryData.add( EmaFactory.createFieldEntry().real(31, askSize, MagnitudeType.EXPONENT_0))
    entryData.add(EmaFactory.createFieldEntry().uintValue(4147,askTimMs))
    entryData.add(EmaFactory.createFieldEntry().uintValue(4150,bidTimMs))
    entryKey = generateEntryKey()
  	map.add(EmaFactory.createMapEntry().keyAscii(entryKey, MapEntry.MapAction.ADD, entryData));
    return map 
  }
}