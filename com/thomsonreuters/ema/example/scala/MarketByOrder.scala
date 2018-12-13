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
import com.thomsonreuters.ema.access.FieldList 
//This class is to update data, create Market by Order's map containing data 
//which will be added in the payload of refresh or update messages
class MarketByOrder {
  //Summary data also contains field id 1709 and 3423 with its fixed value.
  //field id 1 PROD_PERM(PERMISSION) will be added in summary data if refreshPE > 0
  private val dsplyName: String = "DUMMY MBO" //field id 3
  private val currency: Int = 0 //field id 15
  private var activeDate: Calendar = null //field id 17
  
  //The key for each map entry
  private var entryKey: Long = 12102
  
  //MapEntry data also contains field id 6522 
  //which is the same value as the activeDate variable
  private var orderId: Long = 1072130 //field id 3426
  private var orderPrc: Int = 7925 //field id 3427, 2 decimals
  private var orderSide: Short = 1 //field id 3428, BID(1) or ASK(2)
  private var orderSize: Int = 500 //field id 3429
  private var prTimMs: Long =  4800050//field id 6520

  
  //The method sets new value of each field in a refresh and an update message
  def updateData() {
    orderId = orderId +10
    orderPrc = orderPrc + 121
    if(orderSide == 1)
      orderSide = 2
    else 
      orderSide = 1
    orderSize= orderSize + 100
    prTimMs = prTimMs + 1000
    entryKey = entryKey + 9
  }
  
  //The method returns Market by Order data contained in the payload of a refresh message
  def generateMBORefresh(refreshPE: Long, update: Boolean) : Map =   {
    //updateData() is invoked after fail over is successful, 
    //then creates the payload of a refresh message with new values
    //otherwise, use the initialized values
    if(update)
        updateData()
        
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
		mapSummaryData.add(EmaFactory.createFieldEntry().enumValue(1709,  158));
		mapSummaryData.add(EmaFactory.createFieldEntry().enumValue(3423,  1));
		val map: Map = EmaFactory.createMap();
		map.summaryData(mapSummaryData);

		//Create a map containing 5 map entries
		for( a <- 1 to 5) {
		  val entryData: FieldList = EmaFactory.createFieldList()
		  entryData.add(EmaFactory.createFieldEntry().rmtes(3426, ByteBuffer.wrap(orderId.toString().getBytes())));
  		entryData.add(EmaFactory.createFieldEntry().real(3427, orderPrc, MagnitudeType.EXPONENT_NEG_2))		
  		entryData.add(EmaFactory.createFieldEntry().enumValue(3428, orderSide));
  	  entryData.add(EmaFactory.createFieldEntry().real(3429, orderSize, MagnitudeType.EXPONENT_0))
  	  entryData.add(EmaFactory.createFieldEntry().uintValue(6520,prTimMs))
  		entryData.add( EmaFactory.createFieldEntry().date(6522, 
  		              //month is zero based so + 1
                    activeDate.get(Calendar.YEAR), activeDate.get(Calendar.MONTH) +1 , activeDate.get(Calendar.DATE)))
  		map.add(EmaFactory.createMapEntry().keyAscii(entryKey.toString(), MapEntry.MapAction.ADD, entryData));
  	  if (a < 5)
			  updateData();
		}
    return map 
   }
  
  //The method returns Market by Order data contained in the payload of an update message
  def generateMBOUpdate() : Map =   {
    //Set new value to each field
    updateData();
    
    //Create a map containing 1 map entry
		val map: Map = EmaFactory.createMap();
		val entryData: FieldList = EmaFactory.createFieldList()
		entryData.add(EmaFactory.createFieldEntry().rmtes(3426, ByteBuffer.wrap(orderId.toString().getBytes())));
  	entryData.add(EmaFactory.createFieldEntry().real(3427, orderPrc, MagnitudeType.EXPONENT_NEG_2))		
  	entryData.add(EmaFactory.createFieldEntry().enumValue(3428, orderSide));
  	entryData.add(EmaFactory.createFieldEntry().real(3429, orderSize, MagnitudeType.EXPONENT_0))
  	entryData.add(EmaFactory.createFieldEntry().uintValue(6520,prTimMs))
  	entryData.add( EmaFactory.createFieldEntry().date(6522, 
  		              //month is zero based so + 1
                    activeDate.get(Calendar.YEAR), activeDate.get(Calendar.MONTH) +1 , activeDate.get(Calendar.DATE)))
  	map.add(EmaFactory.createMapEntry().keyAscii(entryKey.toString(), MapEntry.MapAction.ADD, entryData));
    return map 
  }
}