package com.thomsonreuters.ema.example.scala
import com.thomsonreuters.ema.access.EmaFactory 
import com.thomsonreuters.ema.access.FieldList 
import com.thomsonreuters.ema.access.OmmReal.MagnitudeType
import com.thomsonreuters.upa.dacs._
import javax.xml.bind.DatatypeConverter
import java.nio.ByteBuffer
import com.thomsonreuters.ema.access.EmaFactory
import com.thomsonreuters.ema.access.FieldList
import com.thomsonreuters.ema.access.OmmException
import com.thomsonreuters.ema.access.OmmNiProviderConfig
import com.thomsonreuters.ema.access.OmmProvider
import com.thomsonreuters.ema.access.OmmReal
import com.thomsonreuters.ema.access.OmmState
import com.thomsonreuters.ema.access.RefreshMsg
import com.thomsonreuters.ema.access.UpdateMsg
import com.thomsonreuters.ema.rdm.EmaRdm
//callback interface for niprovider
import com.thomsonreuters.ema.access.OmmProviderClient
import com.thomsonreuters.ema.access.OmmProviderEvent
import com.thomsonreuters.ema.access.StatusMsg
import com.thomsonreuters.ema.access.GenericMsg
import com.thomsonreuters.ema.access.Msg
import com.thomsonreuters.ema.access.PostMsg
import com.thomsonreuters.ema.access.ReqMsg
class AppClient extends OmmProviderClient {
  //The variable keeping the connection state between non-interactive provider(NIP) and ADH
  //if the connection is ready to publish. 
  //true is connection up and NIP logs in successfully otherwise the variable is false.
  var _connectionUp: Boolean = false
	
	def isConnectionUp(): Boolean = {
		return _connectionUp;
	}
		
	def onRefreshMsg(refreshMsg: RefreshMsg, event: OmmProviderEvent)
	{
    println(refreshMsg);
		//After NIP can connect to ADH and logs in, 
    //it receives a message indicating that the login is successful.
		if ((refreshMsg.state().streamState() == OmmState.StreamState.OPEN) 
		    && (refreshMsg.state().dataState() == OmmState.DataState.OK))
		  _connectionUp = true;
		//otherwise fail to connect or logs in to ADH
		else 
		  _connectionUp = false;
	}
	
	def onStatusMsg(statusMsg: StatusMsg , event: OmmProviderEvent) 
	{
	 
	  println(statusMsg);
		if (statusMsg.hasState())
		{
		   //After NIP can connect to ADH and logs in, 
      //it receives a message indicating that the login is successful.
			if (( statusMsg.state().streamState() == OmmState.StreamState.OPEN) 
			    && (statusMsg.state().dataState() == OmmState.DataState.OK))
				  _connectionUp = true
			//otherwise fail to connect or logs in to ADH
		   else 
		     _connectionUp = false	
		}
	}
	
	def onGenericMsg(genericMsg: GenericMsg, event: OmmProviderEvent){}
	def onAllMsg(msg:Msg ,event:  OmmProviderEvent){}
	def onPostMsg(postMsg: PostMsg,providerEvent:  OmmProviderEvent) {}
	def onReqMsg(reqMsg: ReqMsg, providerEvent: OmmProviderEvent) {}
	def onReissue(reqMsg: ReqMsg, providerEvent: OmmProviderEvent) {}
	def onClose(reqMsg: ReqMsg ,providerEvent:  OmmProviderEvent) {}
}

object NiProvider {
    //The application parameters
    var nipNode: String = "Provider_1"
    var service: String = "NI_PUB"
    var user: String = "user"
    var itemName: String = "DUMMY.N"
    var updateTimes: Int = 10  //publish 10 update messages
    var updateInterval: Int = 5  //seconds 
    //refreshPE is PROD_PERM field(field id 1)
    var refreshPE: Long = 0 //no DACS lock is generated
    var dacsServiceId: Int = 0 //no DACS lock is generated
    
    //Permission data is the permission information associated with content of this stream.
    //It consists of dacsServiceId and refreshPE
    var permData:DacsLock = null
    //Permission data is optional by default
    var addPermissionData: Boolean = false
    val NO_PERMISSSON_PRODPERM: String = "There is no PermissionData and field PROD_PERM added in the Refresh Message."
    
    //Each instance provides data of its domain type
    val mp: MarketPrice = new MarketPrice
    val mbo: MarketByOrder = new MarketByOrder
    val mbp: MarketByPrice = new MarketByPrice
    val mm: MarketMaker = new MarketMaker
    //The default domain type is Market Price
    var domainType: Int = EmaRdm.MMT_MARKET_PRICE
    //List of domain types matches the input domainType
    val domainTypeMap = Map(EmaRdm.MMT_MARKET_PRICE -> "MARKET_PRICE" ,  EmaRdm.MMT_MARKET_BY_ORDER->"MARKET_BY_ORDER" , EmaRdm.MMT_MARKET_BY_PRICE ->  "MARKET_BY_PRICE", EmaRdm.MMT_MARKET_MAKER ->  "MARKET_MAKER" )
    //Each domain type matches the method which generate its payload contained in the refresh message.
    val domainTypeMapRefresh= Map((EmaRdm.MMT_MARKET_PRICE -> mp.generateMPRefresh _),(EmaRdm.MMT_MARKET_BY_ORDER -> mbo.generateMBORefresh _),(EmaRdm.MMT_MARKET_BY_PRICE -> mbp.generateMBPRefresh _), (EmaRdm.MMT_MARKET_MAKER -> mm.generateMMRefresh _))
   //Each domain type matches the method which generate its payload contained in the update message.
    val domainTypeMapUpdate= Map((EmaRdm.MMT_MARKET_PRICE -> mp.generateMPUpate _),(EmaRdm.MMT_MARKET_BY_ORDER -> mbo.generateMBOUpdate _),(EmaRdm.MMT_MARKET_BY_PRICE -> mbp.generateMBPUpdate _), (EmaRdm.MMT_MARKET_MAKER -> mm.generateMMUpdate _) )
     
    val itemHandle: Long = 5
     
  //Utility method show application help message.
  def showHelp() {
    val help: String =  
         "command option list:\n" +
        "    -nipNode <NiProviderName>         					The Name of NiProvider node in EmaConfig.xml. The default is Provider_1.\n" +
        "									The node contains the ip and port of ADH server(s) where the application connects to\n" +
        "    -service <service_name>           					The Service name. The default is NI_PUB.\n" +
        "    -user <name>                      					The Name of application user. The default is user.\n" +
        "    -domainType <domain_type>         					The Domain Type of the published item. The default is MARKET_PRICE.\n" +
        "                                     	 				The valid Domain Types supported by this application are:\n" +
        "                                      					MARKET_PRICE, MARKET_BY_ORDER, MARKET_BY_PRICE, MARKET_MAKER.\n" +
        "    -itemName <a RIC>                 					The published RIC name. The default is DUMMY.N.\n"  + 
        "    -updateTimes <number_updates>     					The number of updates to be published. The default is 10 updates\n" +
        "    -updateInterval <update_interval_in_sec>   The Update interval in seconds. The default is 5 seconds\n" +
        "    -refreshPE <a PE>                 					a PE for refresh messages; This requires dacsServiceId. The default is no PE.\n" +
        "    -dacsServiceId <a service id>     					The serviceID to generate DACS lock for the PE. The default is no serviceID\n"
        println(help) 
        System.exit(1)
  }
  //Extract the application parameters from the command line.
  def getCommandLineOptions(args:Array[String]) {
    try {
      var i: Int = 0
      while( i < args.length) { 
        if (args(i).equalsIgnoreCase("-nipNode")) {
           i += 1
           nipNode = args(i)
        }
        else if (args(i).equalsIgnoreCase("-service")) {
           i += 1
          service = args(i)
        }
        else if (args(i).equalsIgnoreCase("-user")) {
           i += 1
           user = args(i)
        }
        else if (args(i).equalsIgnoreCase("-domainType")) {
           i += 1
           var isValidDomainType: Boolean = false
           for ((domainId,domainName) <- domainTypeMap) {
              if(args(i).toUpperCase().equals(domainName) && !isValidDomainType) {
                domainType = domainId
                isValidDomainType = true
              }
           } 
           if(!isValidDomainType) {
              //if it is invalid domain type, print the valid types then exits.
              println("Error: The domainType " + args(i) + " is not supported by this application. The valid domainType are:");
              for ((domainId,domainName) <- domainTypeMap) {
                print(domainName + ", ")
             }
             println()
             System.exit(1);
          }
        }
        else if (args(i).equalsIgnoreCase("-itemName")) {
           i += 1
           itemName = args(i)
        }
        else if (args(i).equalsIgnoreCase("-updateTimes")) {
           i += 1
           try {
             if((args(i).toInt) <= 0) {
               println("Warning: The updateTimes=" + args(i) + " is invalid so the default updateTimes," + updateTimes + " times, is used.")
             } else {
               updateTimes = args(i).toInt
             }
           }
           catch  {
              case e: NumberFormatException => {
                   println("Warning: The updateTimes=" + args(i) + " is invalid so the default updateTimes," + updateTimes + " times, is used.")
             }
           }
        }
        else if (args(i).equalsIgnoreCase("-updateInterval")) {
           i += 1
           try {
             if((args(i).toInt) <= 0) {
               println("Warning: The updateInterval=" + args(i) + " is invalid so the default updateInterval," + updateInterval + " seconds, is used.")
             } else {
               updateInterval = args(i).toInt
             }
           }
           catch  {
              case e: NumberFormatException => {
                 println("Warning: The updateInterval=" + args(i) + " is invalid so the default updateInterval," + updateInterval + " seconds, is used.")
             }
           }
        }
        else if (args(i).equalsIgnoreCase("-refreshPE")) {
           i += 1
           try {
             refreshPE = args(i).toLong
             if(refreshPE <= 0) {
               println("Warning: The refreshPE=" + args(i) + " is invalid so " + NO_PERMISSSON_PRODPERM)
             } 
           }
           catch  {
              case e: NumberFormatException => {
                 println("Warning: The refreshPE=" + args(i) + " is invalid so " + NO_PERMISSSON_PRODPERM)
             }
           }
        } 
        else if (args(i).equalsIgnoreCase("-dacsServiceId")) {
           i += 1
           try {
             dacsServiceId = args(i).toInt
             if(dacsServiceId <= 0) {
               println("Warning: The dacsServiceId=" + args(i) + " is invalid so " + NO_PERMISSSON_PRODPERM)
             } 
           }
           catch  {
              case e: NumberFormatException => {
                 println("Warning: The dacsServiceId=" + args(i) + " is invalid so " + NO_PERMISSSON_PRODPERM)
             }
           }
        }
        i += 1;
      }
    }
    catch  {
      case e: ArrayIndexOutOfBoundsException => {
           println("Error: Invalid program parameter(s)");
           showHelp();
      }
    }
   //if PE and the service id are valid
   if(refreshPE > 0 && dacsServiceId > 0) {
       //permData and field PROD_PERM are added in the refresh message
       addPermissionData = true;
    } else { //if one of them or both is not valid
      //permData and field PROD_PERM are not added in the refresh message
       addPermissionData = false;
    }
  }
    
    //Create DACS Lock according to the service id and one PE
    def createDacsLock(dacsServiceId: Int, aPE: Long):DacsLock = {
      val _dacsInterface: JDacsLock = JDacsLock.createJDacsLock();
      val _error: DacsError = JDacsLock.createDacsError();
      val peList: Array[Long] = Array(aPE)
      // calculate the length of the new lock
      val len: Int = _dacsInterface.calculateLockLength(dacsServiceId, 
                                    DacsOperations.OR_OPERATION,peList,1,_error) 
      if(len <= 0) {
          println("calculate PermissionData's length failed " + _error.errorId() + _error.text())
          return null;
      }
        
      // create the lock object
      val lock1: DacsLock = JDacsLock.createLock()
      // use the calculated lock data length to get the ByteBuffer from pool
      val lockData: ByteBuffer = ByteBuffer.allocate(len)
      //set allocated ByteBuffer to DacsLock 
      lock1.data(lockData)
      // populate the DacsLock according to the service id and a PE
      val ret: Int = _dacsInterface.createLock(dacsServiceId,
                                    DacsOperations.OR_OPERATION,peList,1,lock1,_error)
      //if the DacsLock is created successfully, return DacsLock
      if (ret == DacsReturnCodes.NO_ERROR) {
        return lock1
      }//if fail, return null
       else { 
        println("created PermissionData's failed " + _error.errorId() + _error.text())
        return null 
      }
      
    }
    def showAllParameters() {
     println("The application is using the following parameters:")
     println("nipNode="+nipNode)
     println("service="+service)
     println("user="+user)
     println("domainType="+domainTypeMap(domainType))
     println("itemName="+itemName)
     println("updateTimes="+updateTimes)
     println("updateInterval="+updateInterval)
     println("refreshPE="+refreshPE)
     println("dacsServiceId="+dacsServiceId)
     println();
  }
   def main (args:Array[String]) {
     getCommandLineOptions(args);
     showAllParameters();
     var config: OmmNiProviderConfig = null
     var provider: OmmProvider = null
     val nipClient: AppClient  = new AppClient()
     var sendRefreshMsg:Boolean = false
     try {
        //Read a Non-Interactive Provider configuration's node named nipNode's value in EmaConfig.xml
        //the node should contain the ChannelSet parameter for fail over process 
        //Hence, when the first ADH is down, EMA tries to connect to the second ADH defined in the ChannelSet parameter 
        config  = EmaFactory.createOmmNiProviderConfig().providerName(nipNode);
        
        //add the second parameters, nipClient instance, containing the callback methods to process events e.g. login events
        //The events are generated when the login stream changes including the change of the connection state(connection is up or down)  
        provider = EmaFactory.createOmmProvider(config.username(user).operationModel(OmmNiProviderConfig.OperationModel.USER_DISPATCH),nipClient)
        
        //dispatch the login events
        provider.dispatch( 1000000 );
        
        //create a refresh message according to the input domain type
        val aRefreshMsg: RefreshMsg  = EmaFactory.createRefreshMsg().serviceName(service).name(itemName).domainType(domainType)
					.state(OmmState.StreamState.OPEN, OmmState.DataState.OK, OmmState.StatusCode.NONE, "UnSolicited Refresh Completed")
					.complete(true);
        
       //check if permissionData/DacsLock must be created
       if(addPermissionData) {
         //if yes, create a DacsLock according to a service id and a PE
		     permData = createDacsLock(dacsServiceId,refreshPE)
       }
       //if permissionData/DacsLock is created successfully
		   if(permData != null) {
		      //add PROD_PERM field(refreshPE) and other fields according to the domain type in the payload of refresh message
		     //the values of all fields are the initialized values(false)
          aRefreshMsg.payload(domainTypeMapRefresh(domainType)(refreshPE,false))
          //add permissionData/DacsLock in the refresh message
		      aRefreshMsg.permissionData(permData.data());
		   }
		   //if permissionData/DacsLock is not created or failed to create it.
		   else {
		     //inform the refresh message does not have permData and PROD_PERM field
		     println(NO_PERMISSSON_PRODPERM)
		     //not add PROD_PERM field(0) 
		     //but add other fields according to the domain type in the payload of refresh message
		     //the values of all fields are the initialized values(false)
		     aRefreshMsg.payload(domainTypeMapRefresh(domainType)(0,false))
		   }
		   //publish the first data message, refresh message
			 provider.submit( aRefreshMsg, itemHandle);
			 println("a Refersh Msg has been published");
			 //waiting for ADH acknowledges that the nip service is up and get the refresh message
			 Thread.sleep(5000);
			 var anUpdateMsg: UpdateMsg = null
			 var num: Int = 0
			 println("Start publishing " + updateTimes + " updates every " + updateInterval + " seconds.");
			 while(num < updateTimes) { //publish the number of update messages according to the updateTimes parameter
			    //if the next sent message is an update message(not refresh message)
			    if(sendRefreshMsg == false) {
			      //wait updateInterval seconds before publishing an update message 
			      Thread.sleep(updateInterval*1000) 
			    }
			    //Let EMA dispatches an event when there is any connection state changes(up to down or down to up). 
			    provider.dispatch( 1000000 ); 
			    //if ADH is ready to be published; the connection state is up
			    if ( nipClient.isConnectionUp())
				  { 
				    if ( sendRefreshMsg ) 
				    // when fail over from down ADH to up ADH is successful, re-send a refresh message to up ADH
					  {
				        //create a refresh message with new values(true)
				        aRefreshMsg.payload(domainTypeMapRefresh(domainType)(refreshPE,true))
				        //publish a refresh message
				        provider.submit( aRefreshMsg, itemHandle);
				        //mark flag not to send a refresh message again
			          sendRefreshMsg = false
					  }//Otherwise, send update message of the the input domain type.
			      //and increase the number of published update messages 
				    else {
			        anUpdateMsg =  EmaFactory.createUpdateMsg().serviceName(service).name(itemName).domainType(domainType)
			        anUpdateMsg.payload(domainTypeMapUpdate(domainType)())
			        provider.submit( anUpdateMsg, itemHandle)  
			        num += 1
				    }
				  }
			    //if ADH is not ready to be published(connection state is down),
			    //wait EMA tries to connect to the next ADH specified in ChannelSet
			    //and mark flag to send a refresh message after connect to the next ADH successfully
			    else {
			      sendRefreshMsg = true;
			    }
			 }
			 println("All " + updateTimes + " updates have been published ");
			 println("Close Stream item " + itemName);
			 //Send close status message to close the item stream.
			 provider.submit( EmaFactory.createStatusMsg().serviceName(service).name(itemName).domainType(domainType).state(OmmState.StreamState.CLOSED, OmmState.DataState.SUSPECT,
					OmmState.StatusCode.NONE, itemName + " Stream Closed"), itemHandle);
			 //Wait for that consumer applications will receive close stream item message after the last update message
			 Thread.sleep(10000);
			}
      catch {
        case excp @ (_: InterruptedException | _: OmmException) =>
          println("Exception:" +excp.getMessage)

      } finally if (provider != null) 
        //log out and disconnect from ADH 
        provider.uninitialize()
			
   }
}
