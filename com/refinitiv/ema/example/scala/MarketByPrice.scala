package com.refinitiv.ema.example.scala
import com.refinitiv.ema.access.OmmReal.MagnitudeType
import com.refinitiv.ema.access.EmaFactory
import com.refinitiv.ema.access.FieldList 
import com.refinitiv.ema.access.FieldEntry
import com.refinitiv.ema.access.Map
import com.refinitiv.ema.access.MapEntry
import java.util.Calendar
import java.nio.ByteBuffer;
import java.util.TimeZone
//This class is to update data, create Market by Price's map containing data 
//which will be added in the payload of refresh or update messages
class MarketByPrice {
  //Summary data 
  //field id 1 PROD_PERM(PERMISSION) will be added in summary data if refreshPE > 0
  private val dsplyName: String = "DUMMY MBP" //field id 3
  private val currency: Int = 764 //field id 15, 764 is Thai baht
  private var activeDate: Calendar = null //field id 17
  private var seqNum: Int = 450596 //field id 1021
  private var timActMs: Long = 30802966 //field id 4148
  
  //The key for each map entry
  private var entryKey: String = "113.05A" //it is orderPrc + A e.g. 113.05A
  
  //MapEntry data
  private var orderPrc: Int = 11305 //field id 3427, 2 decimals so it is 113.05
  private var orderSide: Short = 1 //field id 3428, BID(1) or ASK(2)
  private var noOrd: Int = 1 //field id 3430
  private var accSize: Int = 1525//field id 4356
  private var lvTimMs: Long = 18009780 //field id 6527
  
 //The method sets new value of each field in a refresh and an update message
  def updateData(updatedSum: Boolean) {
    //if updateSum is true, summary data's fields in a message are new values 
    if(updatedSum) {
      timActMs = timActMs +1000
      seqNum = seqNum + 25
    }
    
    orderPrc = orderPrc + 213
    if(orderSide == 1)
      orderSide = 2
    else 
      orderSide = 1
    accSize= accSize + 147
    noOrd = noOrd + 1
    lvTimMs = lvTimMs + 1000
    
    //generate entry key based on orderPrc
    entryKey = orderPrc.toString().substring(0,3) + "." +orderPrc.toString().substring(3) + "A"
  }
   //The method returns Market by Price data contained in the payload of a refresh message
  def generateMBPRefresh(refreshPE: Long, update: Boolean) : Map =   {
    //updateData() is invoked after fail over is successful, 
    //then creates the payload of a refresh message with new values
    //otherwise, use the initialized values
    if(update)
        updateData(true)
        
    //Create summary data
    val mapSummaryData: FieldList  = EmaFactory.createFieldList();
    if(refreshPE > 0) {
        mapSummaryData.add( EmaFactory.createFieldEntry().uintValue(1, refreshPE))
     }
    
    mapSummaryData.add( EmaFactory.createFieldEntry().rmtes(3, ByteBuffer.wrap(dsplyName.getBytes())))
		mapSummaryData.add(EmaFactory.createFieldEntry().enumValue(15,  764));
    activeDate = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    mapSummaryData.add( EmaFactory.createFieldEntry().date(17, 
                  //month is zero based so + 1
                  activeDate.get(Calendar.YEAR), activeDate.get(Calendar.MONTH) +1, activeDate.get(Calendar.DATE)))
		mapSummaryData.add(EmaFactory.createFieldEntry().real(1021,seqNum,MagnitudeType.EXPONENT_0))
		mapSummaryData.add(EmaFactory.createFieldEntry().uintValue(4148, timActMs))
		val map: Map = EmaFactory.createMap()
		map.summaryData(mapSummaryData);
		
    //Create a map containing 5 map entries
		for( a <- 1 to 5) {
		  val entryData: FieldList = EmaFactory.createFieldList()
  		entryData.add(EmaFactory.createFieldEntry().real(3427, orderPrc, MagnitudeType.EXPONENT_NEG_2))		
  		entryData.add(EmaFactory.createFieldEntry().enumValue(3428, orderSide));
  	  entryData.add(EmaFactory.createFieldEntry().uintValue(3430, noOrd))
  	  entryData.add(EmaFactory.createFieldEntry().real(4356,accSize,MagnitudeType.EXPONENT_0))
  		entryData.add( EmaFactory.createFieldEntry().uintValue(6527, lvTimMs))
  		map.add(EmaFactory.createMapEntry().keyAscii(entryKey, MapEntry.MapAction.ADD, entryData));
  	  if (a < 5)
			  updateData(false)
		}
    return map 
   }
  
  //The method returns Market by Price data contained in the payload of an update message
  def generateMBPUpdate() : Map =   {
    //Set new value to each field
    updateData(true)
    
    //Create summary data
    val mapSummaryData: FieldList  = EmaFactory.createFieldList();
    mapSummaryData.add(EmaFactory.createFieldEntry().real(1021,seqNum,MagnitudeType.EXPONENT_0))
		mapSummaryData.add(EmaFactory.createFieldEntry().uintValue(4148, timActMs))
		val map: Map = EmaFactory.createMap();
    map.summaryData(mapSummaryData);
    
     //Create a map one map entry
		val entryData: FieldList = EmaFactory.createFieldList()
		entryData.add(EmaFactory.createFieldEntry().real(3427, orderPrc, MagnitudeType.EXPONENT_NEG_2))		
    entryData.add(EmaFactory.createFieldEntry().enumValue(3428, orderSide));
  	entryData.add(EmaFactory.createFieldEntry().uintValue(3430, noOrd))
  	entryData.add(EmaFactory.createFieldEntry().real(4356,accSize,MagnitudeType.EXPONENT_0))
  	entryData.add( EmaFactory.createFieldEntry().uintValue(6527, lvTimMs))
  	map.add(EmaFactory.createMapEntry().keyAscii(entryKey, MapEntry.MapAction.ADD, entryData));
    return map 
  }
}