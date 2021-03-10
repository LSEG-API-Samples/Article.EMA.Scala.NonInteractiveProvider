package com.refinitiv.ema.example.scala
import scala.collection.mutable.HashMap
import com.refinitiv.ema.access.OmmReal.MagnitudeType
import com.refinitiv.ema.access.EmaFactory
import com.refinitiv.ema.access.FieldList 
import java.nio.ByteBuffer

//This class is to update data, create Market Price's field list containing data 
//which will be added in the payload of refresh or update messages
class MarketPrice {
  //Refresh message contains all fields below
  //field id 1 PROD_PERM(PERMISSION) will be added in the refresh message if refreshPE > 0
  private var dsplyName: String = "DUMMY MP" //field id 3
  private var currency: Int = 764 //field id 15, 764 is Thai baht
  private var bid: Int = 1875 //field id 22, 2 decimals
  private var ask: Int = 1901 //field id 25, 2 decimals
  private var bidSize: Int = 221900 //field id 30
  private var askSize: Int = 125920 //field id 31
  
  //The method sets new value of each field in a refresh and an update message
  def updateData() {
    bid = bid + 111;
    ask = ask + 121;
    bidSize = bidSize + 120
    askSize= askSize + 110
  }
  
  //The method returns Market Price data contained in the payload of a refresh message
  def generateMPRefresh(refreshPE: Long, update: Boolean) : FieldList =   {
      //updateData() is invoked after fail over is successful, 
      //then creates the payload of a refresh message with new values
      //otherwise, use the initialized values
      if(update)
        updateData()
        
      //Create a field list  
      val fieldList:FieldList = EmaFactory.createFieldList();
      if(refreshPE > 0) {
        fieldList.add( EmaFactory.createFieldEntry().uintValue(1, refreshPE))
      }      
      fieldList.add( EmaFactory.createFieldEntry().rmtes(3, ByteBuffer.wrap(dsplyName.getBytes())))
      fieldList.add( EmaFactory.createFieldEntry().enumValue(15,  764))
      fieldList.add( EmaFactory.createFieldEntry().real(22, bid, MagnitudeType.EXPONENT_NEG_2))
      fieldList.add( EmaFactory.createFieldEntry().real(25, ask, MagnitudeType.EXPONENT_NEG_2))
      fieldList.add( EmaFactory.createFieldEntry().real(30,bidSize,MagnitudeType.EXPONENT_0))
      fieldList.add( EmaFactory.createFieldEntry().real(31, askSize, MagnitudeType.EXPONENT_0))
      return fieldList 
   }
  
  //The method returns Market Price data contained in the payload of an update message
  def generateMPUpate() : FieldList =   {
      //Set new value to each field
      updateData()
      
      //Create a field list
      val fieldList:FieldList = EmaFactory.createFieldList();   
      fieldList.add( EmaFactory.createFieldEntry().real(22, bid, MagnitudeType.EXPONENT_NEG_2))
      fieldList.add( EmaFactory.createFieldEntry().real(25, ask, MagnitudeType.EXPONENT_NEG_2))
      fieldList.add( EmaFactory.createFieldEntry().real(30,bidSize,MagnitudeType.EXPONENT_0))
      fieldList.add( EmaFactory.createFieldEntry().real(31, askSize, MagnitudeType.EXPONENT_0))
      return fieldList 
   }
  
  
}
