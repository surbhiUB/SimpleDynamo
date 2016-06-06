package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;



public class SimpleDynamoProvider extends ContentProvider {

	private static final int SERVER_PORT = 10000;

	private  String myPort ="";

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	Uri url;

	static HashMap<String,String> ringMap = new HashMap<String, String>();

	static ArrayList<String> ring = new ArrayList<String>();

	static ConcurrentHashMap<String,String> providerMap = new ConcurrentHashMap<String,String>();

	private String waitFlag = "false";

	public static MatrixCursor cursor;


	HashMap<String,String> successorPortMap = new HashMap<String, String>();



	public String isRecovered = "false";

	private static ArrayList<String> portNumbers = new ArrayList<String>();


	public static String  hashedId = "";

	private String recoveryComplete = "true";


	@Override
	public  int delete(Uri uri, String selection, String[] selectionArgs) {

		Context context = this.getContext();

		// TODO Auto-generated method stub
		if(selection.equals("@")) {

			Log.d(TAG, "Local all delete received");
			deleteLocalAll();

		}/*else if(selection.equals("*")){

			Log.d(TAG,"Global delete received");
			//return globalQuery();
		}*/
		else{

			Log.d("Delete","received for key: "+selection);
			/// check which port the query belongs to
			String keyId = genHash(selection);
			String coordinator = getCoordinatorNode(keyId);

			Log.d("Coord Port for key: ", selection+","+coordinator);

			if(myPort.equals(coordinator)){
				// delete key from my partition
				Log.d(TAG,"Delete file for key :"+selection);
				//(this) {
				context.deleteFile(selection);
				providerMap.remove(selection);
				//}

				//send delete message to successor1 for deleting the replicate value
				Message message = new Message();
				message.setToPortId(ringMap.get("successor1"));
				message.setKeySelection(selection);
				message.setSenderPort(myPort);
				message.setMessageType("DeleteReplicate1");

				startClientTask(message);


				Log.d("Delete","Delete replicate1 message sent to port : "+ringMap.get("successor1"));

			}else {
				// send it to port who has the key
				sendDeleteMessage(selection, coordinator);
			}
		}


		return 0;
	}


	public void sendDeleteMessage(String key, String portNo){

		Message deleteMessage = new Message();
		deleteMessage.setToPortId(portNo);
		deleteMessage.setKeySelection(key);
		deleteMessage.setSenderPort(myPort);
		deleteMessage.setMessageType("Delete");

		startClientTask(deleteMessage);
	}


	public void deleteLocalAll(){

		Log.d(TAG, "Delete all files from my avd");
		Context context = this.getContext();
		String fileArr[] = context.fileList();

		for(int i=0; i < fileArr.length;i++) {
			if(!"RECOVERED".equals(fileArr[i]))
				context.deleteFile(fileArr[i]);
		}

		// remove all from providerMap too
		providerMap.clear();
	}



	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public  Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		// if the avd is recovering, block all write request, until recovery finishes
		while(recoveryComplete.equals("false")){

		}

		Log.d("insert", values.toString());

		String key = values.get("key").toString();
		String value = values.get("value").toString();

		Log.d(TAG, "Key: " + key);
		Log.d(TAG, "Value: " + value);

		// hash the key
		String keyId = genHash(key);

		String coordinatorPort = getCoordinatorNode(keyId);

		Log.d("insert","Coordinator port for key: "+key+" is "+coordinatorPort);

		if(myPort.equals(coordinatorPort)){

			// key belongs to my port, save it in my content provider
			if (key != null && !key.isEmpty() && value != null && !value.isEmpty()) {
				writeToFile(key, value);
				sendReplicateMessage(ringMap.get("successor1"),key,value);
			}
		}else{

			//key does not belong to my port, send it to successor port
			sendInsertMessage(coordinatorPort,key,value);

		}

		return null;
	}


	private void sendInsertMessage(String portNo, String key, String value){

		Message message = new Message();
		message.setMessageType("Insert");
		message.setSenderPort(myPort);
		message.setToPortId(portNo);

		HashMap<String,String> map = new HashMap<String,String>();
		map.put("key", key);
		map.put("value", value);
		message.setKeyValueMap(map);

		Log.d(TAG, "Sending insert message to port: " + portNo);
		startClientTask(message);

	}


	private void sendReplicateMessage(String successorPort,String key, String value){

		Message message = new Message();
		message.setMessageType("Replicate1");
		message.setSenderPort(myPort);
		message.setToPortId(successorPort);

		HashMap<String,String> map = new HashMap<String,String>();
		map.put("key",key);
		map.put("value", value);

		message.setKeyValueMap(map);
		Log.d(TAG, "Sending replicate message to port: " + successorPort);
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

		startClientTask(message);

	}



	private synchronized void writeToFile(String key, String value){

		Log.d(TAG, "Writing to File");

		FileOutputStream fos = null;
		try {

			Log.d("Insert","Key :"+key+"Value:"+value);
			// write data to my content povider.
			fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			fos.write(value.getBytes());
			fos.close();

			// also write to provider map to replicate message to successor nodes:


			providerMap.put(key, value);
			//Log.d("ProviderMap",providerMap.keySet().toString());

		} catch (FileNotFoundException e) {

			e.printStackTrace();

		} catch (IOException e) {

			e.printStackTrace();
		}

	}


	private String getCoordinatorNode(String keyId){

		int  index = 0;

		Iterator<String> itr = ring.iterator();

		while(itr.hasNext()){

			String nodeId = itr.next();

			if(index == 0) {

				int n = ring.size();
				int currentNodeCompare = keyId.compareTo(nodeId);
				String largestId =  ring.get(n - 1);
				int largestIdCompare = keyId.compareTo(largestId);

				if (currentNodeCompare <= 0 || largestIdCompare > 0) {
					return portNumbers.get(0);
				}

			}else{

				String previousId = ring.get(index-1);
				int currentNodeCompare = keyId.compareTo(nodeId);
				int previousNodeCompare = keyId.compareTo(previousId);

				if(currentNodeCompare <= 0 && previousNodeCompare > 0){
					return portNumbers.get(index) ;
				}
			}
			index = index + 1;
			//i++;
		}

		return null;
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		// Get my port details:
		/*** set the portNo***/
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		hashedId = genHash(portStr);

		// Create a server socket as well as a thread (AsyncTask) that listens on the server port
		try{

			// create a hashmap for setting the ring with successor/predecessor values of each node
			initializeRingDetails();

			/** set the content provider uri ****/
			this.url = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

			// test recovery first
			String recovery = checkRecovery();

			if("false".equals(recovery)){
				// create a file and store it in avd
				Log.d("Recovery","This is first start,file not found");

				String key = "RECOVERED";
				String value = "RECOVERED";
				writeToFile(key, value);

				Log.d("Recovery", "File recovered stored for first time");

			}else{
				isRecovered = "true";
			}

			if("true".equals(isRecovered)){

				Log.d("Recovery", "AVD has recovered");
				// set the recoveryCompleteStatus
				recoveryComplete = "false";
				sendRecoveryMessages();

			}

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);


		}catch(Exception e){
			e.printStackTrace();
			Log.e(TAG, "Can't create a ServerSocket");
		}

		return false;
	}

	private void sendRecoveryMessages(){

		Message recoveryMessage1 = new Message();
		recoveryMessage1.setMessageType("recovery");
		recoveryMessage1.setSenderPort(myPort);
		recoveryMessage1.setToPortId(ringMap.get("successor1"));
		startClientTask(recoveryMessage1);


		// send recovery message to successor2
		Message recoveryMessage2 = new Message();
		recoveryMessage2.setToPortId(ringMap.get("successor2"));
		recoveryMessage2.setSenderPort(myPort);
		recoveryMessage2.setMessageType("recovery");
		startClientTask(recoveryMessage2);

		// send replica recovery message
		Message replicaRecovery = new Message();
		replicaRecovery.setMessageType("replicaRecover");
		replicaRecovery.setSenderPort(myPort);
		Log.d("Recovery", "sending replicarecover to predecessor1: " + ringMap.get("predecessor1"));
		replicaRecovery.setToPortId(ringMap.get("predecessor1"));
		startClientTask(replicaRecovery);

		Message replicaRecovery2 = new Message();
		replicaRecovery2.setMessageType("replicaRecover");
		replicaRecovery2.setSenderPort(myPort);
		Log.d("Recovery", "sending replicarecover to predecessor2: " + ringMap.get("predecessor2"));
		replicaRecovery2.setToPortId(ringMap.get("predecessor2"));
		startClientTask(replicaRecovery2);

	}


	private void startClientTask(Message msg){

		ClientTask clientTask = new ClientTask();
		clientTask.setMessage(msg);
		Thread thread = new Thread(clientTask);
		thread.start();

	}



	private void initializeRingDetails(){


		// add all the portNumbers
		portNumbers.add("11124");
		portNumbers.add("11112");
		portNumbers.add("11108");
		portNumbers.add("11116");
		portNumbers.add("11120");


		ring.add(genHash("5562"));
		ring.add(genHash("5556"));
		ring.add(genHash("5554"));
		ring.add(genHash("5558"));
		ring.add(genHash("5560"));

		if("11108".equals(myPort)){

			ringMap.put("predecessor1","11112");
			ringMap.put("predecessor2", "11124");
			ringMap.put("successor1","11116");
			ringMap.put("successor2","11120");

		}else if("11116".equals(myPort)){

			ringMap.put("predecessor1","11108");
			ringMap.put("predecessor2","11112");
			ringMap.put("successor1","11120");
			ringMap.put("successor2","11124");

		}else if("11120".equals(myPort)){

			ringMap.put("predecessor1","11116");
			ringMap.put("predecessor2","11108");
			ringMap.put("successor1","11124");
			ringMap.put("successor2","11112");

		}else if("11124".equals(myPort)){

			ringMap.put("predecessor1","11120");
			ringMap.put("predecessor2","11116");
			ringMap.put("successor1","11112");
			ringMap.put("successor2","11108");

		}else if("11112".equals(myPort)){

			ringMap.put("predecessor1","11124");
			ringMap.put("predecessor2","11120");
			ringMap.put("successor1","11108");
			ringMap.put("successor2","11116");

		}

		// initialize the successorMap
		successorPortMap.put("11124","11112");
		successorPortMap.put("11112","11108");
		successorPortMap.put("11108","11116");
		successorPortMap.put("11116","11120");
		successorPortMap.put("11120","11124");

	}


	private String checkRecovery(){

		// check if file exists

		String fileExist = "false";

		Context context = this.getContext();

		if(context.getFileStreamPath("RECOVERED").exists()){
			fileExist = "true";
		}


		return fileExist;
	}


	private Uri buildUri(String scheme, String authority) {

		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder) {


		// if recovery is going on, block all query request

		while(recoveryComplete.equals("false")){}

		Log.d("Query","Recovery completed");


		// TODO Auto-generated method stub
		Log.d("selection :", selection);

		if(selection.equals("@")) {

			Log.d(TAG, "Local all query received");
			return queryLocalAll();

		}else if(selection.equals("*")){

			Log.d(TAG,"Global query received");
			return globalQuery();


		}else{

			Log.d("Query","received for key: "+selection);
			/// check which port the query belongs to
			String keyId = genHash(selection);
			String coordinator = getCoordinatorNode(keyId);

			Log.d("Coord Port for key: ", selection+","+coordinator);

			if(myPort.equals(coordinator)){
				// query my partition
				return queryMyProvider(selection);
			}else {
				// send it to port who has the key
				return sendQueryMessage(selection, coordinator);
			}
		}

		//return null;
	}


	private Cursor globalQuery(){

		// get all data from my provider

		String[] fileArr = getContext().fileList();

		Log.d(TAG, "------File list retrieved for this AVD----- :" + fileArr);

		FileInputStream inputStream;
		BufferedReader bufferedReader;
		HashMap<String,String> map = new HashMap<String, String>();

		try {

			for (int i = 0; i < fileArr.length; i++) {

				if(!"Recovered".equalsIgnoreCase(fileArr[i])) {

					StringBuilder msg = new StringBuilder();

					inputStream = getContext().openFileInput(fileArr[i]);

					bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
					String line;

					while ((line = bufferedReader.readLine()) != null) {
						msg.append(line);
					}

					bufferedReader.close();
					inputStream.close();

					Log.d(TAG, " Value :" + msg + " retrieved for key :" + fileArr[i]);

					//cursor.addRow(new String[]{fileArr[i], msg.toString()});
					map.put(fileArr[i], msg.toString());

				}

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


		Log.d("GlobalQuery",map.toString());
		// create global query message
		Message message = createGlobalQuery(map);

		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

		startClientTask(message);

		// busywait until data from all other AVD's is received
		busyWait();

		return cursor;
	}

	private Message createGlobalQuery(HashMap<String,String> map){

		Message message = new Message();
		message.setMessageType("QueryGlobal");
		message.setQueryingPort(myPort);
		message.setKeyValueMap(map);
		message.setToPortId(ringMap.get("successor1"));

		return message;
	}


	private void busyWait(){

		// busy wait till a reply is received from the receiver
		// wait till the wait is set to true on server side.
		while(true){
			if("true".equals(waitFlag))
				break;
		}

		// reset wait to false
		Log.d("Busy wait", "over");

		waitFlag = "false";

	}



	private Cursor sendQueryMessage(String selection, String portNo){


		Log.d("Query","sending query message to port: "+portNo);

		//create a query message
		Message msg = new Message();
		msg.setToPortId(portNo);
		msg.setMessageType("Query");
		msg.setSenderPort(myPort);
		msg.setKeySelection(selection);

		startClientTask(msg);

		Log.d("Query","message sent");

		busyWait();

		return cursor;

	}


	private Cursor queryMyProvider(String selection){

		String[] columns= {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(columns);

		FileInputStream inputStream;
		BufferedReader bufferedReader;

		try {

			inputStream = getContext().openFileInput(selection);

			bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
			/*** read the message value from the file ***/
			String line;
			StringBuilder value = new StringBuilder();

			while ((line = bufferedReader.readLine()) != null) {
				value.append(line);
			}

			Log.d(TAG, "Value: " + value + " retrieved for key : " + selection);

			bufferedReader.close();
			inputStream.close();

			cursor.addRow(new String[]{selection, value.toString()});

		}catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}

		return cursor;

	}



	private Cursor queryLocalAll() {

		String[] columns= {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(columns);

		String[] fileArr = getContext().fileList();

		Log.d(TAG, "------File list retrieved for this AVD----- :" + fileArr);

		FileInputStream inputStream;
		BufferedReader bufferedReader;
		try {

			for (int i = 0; i < fileArr.length; i++) {

				StringBuilder msg = new StringBuilder();

				inputStream = getContext().openFileInput(fileArr[i]);

				if(!"Recovered".equalsIgnoreCase(fileArr[i])) {

					bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
					String line;

					while ((line = bufferedReader.readLine()) != null) {
						msg.append(line);
					}

					bufferedReader.close();
					inputStream.close();
					Log.d(TAG, " Value :" + msg + " retrieved for key :" + fileArr[i]);
					cursor.addRow(new String[]{fileArr[i], msg.toString()});
				}

			}

		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return cursor;

	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input){

		MessageDigest sha1 = null;
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	private class ClientTask implements Runnable {

		Message msg;

		public void setMessage(Message newMessage){

			this.msg = newMessage;

		}

		/*public ClientTask(Message newMessage){
            this.msg = newMessage;
		}*/

		private void processReply(Message msg){

			String messageType = msg.getMessageType();

			if("QueryReply".equals(messageType)){
				Log.d("Query reply",msg.getKeySelection());
				Log.d("Query reply","Received value for key:" + msg.getKeyValue());
				HashMap<String,String> map = msg.getKeyValueMap();
				Log.d("Map" ,map.get("key") +":" + map.get("value"));
				String[] columns= {"key", "value"};
				cursor = new MatrixCursor(columns);
				cursor.addRow(new String[]{map.get("key"), map.get("value")});
				waitFlag="true";
				// Log.d("Query reply", "finished");
			}else if("deleteReply".equals(messageType)){

				Log.d("Delete Reply","received from port :"+msg.getSenderPort());

			}else if("replyRecovery".equals(messageType)){
				// start a new thread to copy all data to my content provider
				Log.d("RecoverReply", "recovery reply received from node: " + msg.getSenderPort());
				startRecoveryTask(msg);

			}
		}


		private void startRecoveryTask(Message msg){

			NodeRecovery node = new NodeRecovery();
			node.setMessage(msg);
			new Thread(node).start();

		}

		@Override
		public void run() {

			Log.d("ClientTask", "in run");
			// String queryReply = "";
			ObjectOutputStream out = null;
			ObjectInputStream in = null;
			Message reply = null;


			String remotePort = msg.getToPortId();

			try {

				Socket socket = new Socket();


				Log.d("ClientTask", "remotePort: " + remotePort);

				//socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				//		Integer.parseInt(remotePort));

				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(remotePort)), 2000);

				//out = new PrintWriter(socket.getOutputStream(), true);
				out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));

				Log.d(TAG, " Sending " + msg.getMessageType() + "message to port: " + remotePort);

				// write msg to stream
				out.writeObject(msg);
				out.flush();

				Log.d(TAG, "Message sent");


				in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
				// read message from server
				reply = (Message)in.readObject();

				// closing the socket
				socket.close();

				processReply(reply);

			}catch (EOFException e) {
				Log.e("ClientThread", "EOFException");
				e.printStackTrace();
				handleFailure(remotePort);
			} catch (SocketTimeoutException e) {
				Log.e("ClientThread", "SocketTimeoutException");
				e.printStackTrace();
				handleFailure(remotePort);
			} catch (SocketException e) {
				Log.e("ClientThread", "SocketException");
				e.printStackTrace();
				handleFailure(remotePort);
			}catch (Exception e) {
				e.printStackTrace();
			}
		}


		public void handleFailure(String failedPort) {

			Log.d("Failure","Failed portId  :"+msg.getToPortId());

			//String successorPort = getFirstSuccessors(msg.getToPortId());

			// successorPort for failedPort
			String successorPort = successorPortMap.get(msg.getToPortId());

			Log.d("Failure","Successor Port for failure : "+successorPort);

			if("Insert".equals(msg.getMessageType())){

				/// create replica of it in the successor
					sendReplicateMessage(successorPort, msg.getKeyValueMap().get("key"), msg.getKeyValueMap().get("value"));

			}else if("Replicate1".equals(msg.getMessageType())){

				// send replica to successor2
				sendReplica2(successorPort,msg.getKeyValueMap().get("key"),msg.getKeyValueMap().get("value"));

			}else if("Query".equals(msg.getMessageType())){
				// send query to its successor

				if(successorPort.equals(myPort)){

					while(recoveryComplete.equals("false")){}

					String[] columns= {"key", "value"};
					//Cursor result = queryMyProvider(msg.getKeySelection());
					cursor = new MatrixCursor(columns);
					cursor.addRow(new String[]{msg.getKeySelection(), providerMap.get(msg.getKeySelection())});
					waitFlag="true";

				}else

				   sendQuery(msg.getKeySelection(), successorPort);

			}else if("Delete".equals(msg.getMessageType())){
				// delete replica1 from successor
				sendDeleteMessage(msg.getKeySelection(),successorPort);

			}else if("DeleteReplicate1".equals(msg.getMessageType())){
				// delete replica2 from successor
				sendDeleteReplica2(msg.getKeySelection(),successorPort);


			}else if("QueryGlobal".equals(msg.getMessageType())){

				msg.setToPortId(successorPort);
				startClientTask(msg);

			}

		}


		private void sendQuery(String key, String port){

			while(recoveryComplete.equals("false")){}

			Message msg = new Message();
			msg.setToPortId(port);
			msg.setMessageType("Query");
			msg.setSenderPort(myPort);
			msg.setKeySelection(key);

			startClientTask(msg);

		}


		private void sendDeleteReplica2(String key, String port){

			Message deleteMessage = new Message();
			deleteMessage.setToPortId(port);
			deleteMessage.setKeySelection(key);
			deleteMessage.setSenderPort(myPort);
			deleteMessage.setMessageType("DeleteReplicate2");

			startClientTask(msg);


		}

		private void sendReplica2(String port, String key, String value){

			Message msgReplicate2 = new Message();
			msgReplicate2.setMessageType("Replicate2");
			msgReplicate2.setToPortId(port);

			//
			HashMap<String,String> replicateMap = new HashMap<String, String>();
			replicateMap.put("key", key);
			replicateMap.put("value", value);
			msgReplicate2.setKeyValueMap(replicateMap);
			msgReplicate2.setSenderPort(myPort);

			Log.d(TAG, "Sending replicate2 message to port: " + port);
			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgReplicate2);


			startClientTask(msgReplicate2);


		}


	}


	public class NodeRecovery implements Runnable{

		Message message;

		public void setMessage(Message msg){

			this.message = msg;

		}


		@Override
		public void run() {

			String recoveryType = message.getRecoveryType();
			Log.d("Recovery","recovery type : "+recoveryType);

			if(recoveryType.equals("self")){
				// check if the key belongs to my port

				Log.d("RecoveryMapS", message.getKeyValueMap().keySet().toString());

				checkSelf(message.getKeyValueMap());

			}else if(recoveryType.equals("recoverreplica")){
				// check if the key belongs to my port
				Log.d("RecoveryMapP", message.getKeyValueMap().keySet().toString());
				checkRecovery(message.getKeyValueMap());
			}


			// recovery completed
			//set the status to true
			recoveryComplete = "true";

		}


		public void checkRecovery(HashMap<String,String> map){

			for(Map.Entry<String,String> entry : map.entrySet()){

				String key = entry.getKey();
				String result = compareReplica(key, message.getPredecessorPort(),message.getSenderPort());

				if("true".equals(result)){
					// save it in my provider

					Log.d("Recovery Insert Key : "+key,"Value :"+entry.getValue());
					writeToFile(key, entry.getValue());

				}
			}
		}


		public String compareReplica(String key, String predecesorPort,String senderPort){



			String keyId = genHash(key);

			String predecessorId = genHash(String.valueOf(Integer.parseInt(predecesorPort)/2));
			String senderId = genHash(String.valueOf(Integer.parseInt(senderPort)/2));

			int size  = ring.size();

			if(portNumbers.get(0).equals(senderPort)){

				if(keyId.compareTo(senderId) <= 0 || keyId.compareTo(ring.get(size-1)) > 0){
					return "true";
				}

			}else{

				if(keyId.compareTo(senderId) <= 0 && keyId.compareTo(predecessorId) > 0){
					// key belongs to me
					return "true";
				}
			}

			return "false";

		}


		public void checkSelf(HashMap<String,String> map){

			//Log.d("Recovery self" , "inside checkself");

			for(Map.Entry<String,String> entry : map.entrySet()){
				String key = entry.getKey();
				String result = compareSelf(key);

				//Log.d("Recovery Self", "comparison for key : "+key+ " : "+result);
				if("true".equals(result)){
					// save it in my provider

					writeToFile(key, entry.getValue());
				}
			}
		}



		public String compareSelf(String key){

			String keyId = genHash(key);

			int size  = ring.size();

			// corner case: if i am the first port
			if(portNumbers.get(0).equals(myPort)){

				if(keyId.compareTo(hashedId) <= 0 || keyId.compareTo(ring.get(size-1)) > 0){
					return "true";
				}
			}else{

				/// check if the key is greater than my predecessor
				String predecessor =  ringMap.get("predecessor1");
				String predId = genHash(String.valueOf(Integer.parseInt(predecessor)/2));

				if(keyId.compareTo(hashedId) <= 0 && keyId.compareTo(predId) > 0){
					// key belongs to me
					return "true";
				}
			}

			return "false";

		}

	}


	public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Socket socket = null;

			try {

				while (true) {

					socket = serverSocket.accept();

					InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(inputStreamReader);

					ObjectInputStream input = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));

					Message message = (Message) input.readObject();

					String type = message.getMessageType();

					Log.d(TAG, "Message of type: " + type);

					if ("Replicate1".equals(type)) {

						HashMap<String, String> map = message.getKeyValueMap();
						Message replicate1Reply = handleReplicate1(map);

						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						// sending replicate1 acknowledge to sender port
						Log.d("Replicate1 Reply" ,"Sending replicate1 acknowledgement back to sending port");
						output.writeObject(replicate1Reply);
						output.flush();

					}else if("Replicate2".equals(type)){

						HashMap<String, String> map = message.getKeyValueMap();
						handleReplicate2(map);

						Message replicate2Reply = createDummyReply("Replicate2Done");
						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						// sending replicate2 acknowledge to sender port
						Log.d("Replicate2 Reply" ,"Sending replicate2 acknowledgement back to sending port");
						output.writeObject(replicate2Reply);
						output.flush();

					}else if("Insert".equals(type)){

						HashMap<String, String> map = message.getKeyValueMap();
						handleInsert(map);

						Message insertReply = createDummyReply("insertDone");

						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));

						// sending insert acknowledge to sender port
						Log.d("Insert Reply" ,"Sending insert acknowledgement back to sending port");

						output.writeObject(insertReply);
						output.flush();

					}else if("Query".equals(type)){


						Message queryReply = processQuery(message);
						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));

						// sending insert acknowledge to sender port
						Log.d("Query Reply" ,"Sending query reply back to sending port: "+message.getSenderPort());

						output.writeObject(queryReply);
						output.flush();

					}else if("QueryGlobal".equals(type)){

						if(myPort.equals(message.getQueryingPort())){

							Log.d("Globalquery reply","received");


							// send done message to sending port, so that, that AVD does not think I am failed,

							Message dummyMsg = new Message();
							dummyMsg.setMessageType("doneGlobal");
							ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
							// sending acknowledge to sender port
							Log.d("Global Reply" ,"Sending dummy reply back to sending port");
							output.writeObject(dummyMsg);
							output.flush();

							String[] columns= {"key", "value"};
							cursor = new MatrixCursor(columns);

							HashMap<String,String> dataMap = message.getKeyValueMap();
							for (Map.Entry<String,String> map : dataMap.entrySet()){
								cursor.addRow(new String[]{map.getKey(),map.getValue()});
							}

							waitFlag="true";

						}else {
							Log.d("GlobalQuery","Received global query request from: "+message.getQueryingPort());
							processGlobalQuery(message);
							Message dummyMessage = createDummyReply("globalForwarded");

							ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
							// sending delete acknowledge to sender port
							Log.d("Global Reply" ,"Sending dummy reply back to sending port");
							output.writeObject(dummyMessage);
							output.flush();


						}
					}else if("DeleteReplicate1".equals(type)){

						handleDeleteReplicate1(message);

						Message replicate1Reply = createDummyReply("deleteReply");

						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						// sending delete acknowledge to sender port
						Log.d("DeleteReplicate1 Reply" ,"Sending delete reply back to sending port: "+message.getSenderPort());
						output.writeObject(replicate1Reply);
						output.flush();

					}else if("Delete".equals(type)){

						handleDelete(message);

						Message deleteReply = createDummyReply("deleteReply");

						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						// sending delete acknowledge to sender port
						Log.d("Delete Reply" ,"Sending delete reply back to sending port: "+message.getSenderPort());
						output.writeObject(deleteReply);
						output.flush();

					}else if("DeleteReplicate2".equals(type)){

						handleDeleteReplicate2(message);

						Message replicate2Reply = createDummyReply("deleteReply");

						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						// sending delete acknowledge to sender port
						Log.d("DeleteReplicate2 Reply" ,"Sending delete reply back to sending port: "+message.getSenderPort());
						output.writeObject(replicate2Reply);
						output.flush();

					}else if("recovery".equals(type) || "replicaRecover".equals(type)){

						Message recoverReply = processRecovery(message);
						ObjectOutputStream output = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
						// sending delete acknowledge to sender port
						Log.d("recoverReply" ,"Sending recovery reply back to sending port: "+message.getSenderPort());
						output.writeObject(recoverReply);
						output.flush();
					}

				}

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				//Log.v(TAG, e.getMessage());
				e.printStackTrace();
			} finally {
				if (socket != null) {
					try {
						socket.close();
					} catch (IOException e) {
						Log.v(TAG, e.getMessage());
						e.printStackTrace();
					}
				}
			}

			return null;
		}

	}




	private Message processRecovery(Message msg){

		//send all my data to recovered node
		Message message = new Message();
		message.setMessageType("replyRecovery");
		message.setSenderPort(myPort);
		HashMap<String,String> dataMap = new HashMap<String, String>();

		dataMap.putAll(providerMap);
		message.setKeyValueMap(dataMap);

		if("recovery".equals(msg.getMessageType()))
			message.setRecoveryType("self");
		else
			message.setRecoveryType("recoverreplica");

		message.setPredecessorPort(ringMap.get("predecessor1"));

		return message;
	}


	private Message createDummyReply(String type){

		Message replyMessage = new Message();
		replyMessage.setMessageType(type);
		replyMessage.setSenderPort(myPort);

		return replyMessage;

	}

	private void handleDelete(Message msg){

		Log.d("Delete", "start");

		Context context = this.getContext();
		//synchronized (this) {

		context.deleteFile(msg.getKeySelection());
		providerMap.remove(msg.getKeySelection());

		//}

		Message message = new Message();
		message.setToPortId(ringMap.get("successor1"));
		message.setKeySelection(msg.getKeySelection());
		message.setSenderPort(myPort);
		message.setMessageType("DeleteReplicate1");

		startClientTask(message);

		Log.d("Delete", "Delete replicate1 message sent to port : " + ringMap.get("successor1"));

		Log.d("delete", "done");

		//return replyMessage;

	}



	private void handleDeleteReplicate1(Message msg){

		Log.d("DeleteReplicate1", "start");

		Context context = this.getContext();

		//synchronized (this) {
		context.deleteFile(msg.getKeySelection());
		providerMap.remove(msg.getKeySelection());

		//}

		// send replica2 message to successor1
		Message deleteMessage = new Message();
		deleteMessage.setToPortId(ringMap.get("successor1"));
		deleteMessage.setKeySelection(msg.getKeySelection());
		deleteMessage.setSenderPort(myPort);
		deleteMessage.setMessageType("DeleteReplicate2");

		startClientTask(deleteMessage);

	}

	private void handleDeleteReplicate2(Message msg){

		Log.d("DeleteReplicate2", "start");

		//synchronized (this) {

		Context context = this.getContext();
		context.deleteFile(msg.getKeySelection());
		providerMap.remove(msg.getKeySelection());

		//}


		Log.d("DeleteReplicate2", "done");

		//return replyMessage;

	}




	private void processGlobalQuery(Message msg){

		Log.d("Process Global Query", "start");
		// dump all the data from my provider into the globalMap

		HashMap<String, String> map = null;


		//Log.d("GlobalQuery",map.toString());
		//synchronized (this) {
		map = msg.getKeyValueMap();
		map.putAll(providerMap);
		//}

		// create a global query for my successor
		Message message = new Message();
		message.setMessageType("QueryGlobal");
		message.setKeyValueMap(map);
		message.setQueryingPort(msg.getQueryingPort());
		message.setToPortId(ringMap.get("successor1"));

		// Forwarding the global query to my successor port

		startClientTask(message);

		Log.d("Process Global Query", "done");

		// send the reply back to the sender, otherwise the sender will think that the AVD has failed.


	}


	private  Message processQuery(Message msg){

		/// Key/value pair is also saved in the providerMap

		// blocking any query request from other avds, until I have recovered
		while(recoveryComplete.equals("false")){}

		Log.d("ProcessQuery","start for key: "+msg.getKeySelection());

		while(providerMap.get(msg.getKeySelection()) == null){

		}

		//create a queryReplyMessage to send reply
		Message message = new Message();
		message.setMessageType("QueryReply");
		message.setToPortId(msg.getSenderPort());
		message.setKeySelection(msg.getKeySelection());
		//set key/Value map
		HashMap<String,String> map = new HashMap<String, String>();
		map.put("key", msg.getKeySelection());

		Log.d("ProcessQuery: ", providerMap.keySet().toString());

		Log.d("ProcessQuery :", "Key :" + msg.getKeySelection() + " Value :" + providerMap.get(msg.getKeySelection()));
		map.put("value", providerMap.get(msg.getKeySelection()));

		message.setKeyValue(map.get("value"));
		message.setKeyValueMap(map);

		Log.d("Process Query", "Done for key :" + msg.getKeySelection()+ " Value: "+map.get("value"));


		return message;

	}


	private void handleInsert(HashMap<String,String> map){

		while(recoveryComplete.equals("false")){}

		//synchronized (this) {
		writeToFile(map.get("key"), map.get("value"));
		//}

		// send replicate1 message to successor port
		sendReplicateMessage(ringMap.get("successor1"), map.get("key"), map.get("value"));

		//send insert reply to main sender
		//return message;
	}


	private Message handleReplicate1(HashMap<String,String> map) {

		while(recoveryComplete.equals("false")){}

		Log.d(TAG,"Inside handleReplicate1");


		Log.d(TAG,"Writing to provider");
		writeToFile(map.get("key"), map.get("value"));
		Log.d(TAG, "Writing to provider done");


		// since this is the first successor to receive the replicate message
		//,send the message to next successor2 port
		Message msgReplicate2 = new Message();
		msgReplicate2.setMessageType("Replicate2");
		String successorPort = ringMap.get("successor1");
		msgReplicate2.setToPortId(successorPort);

		//
		HashMap<String,String> replicateMap = new HashMap<String, String>();
		replicateMap.put("key", map.get("key"));
		replicateMap.put("value", map.get("value"));
		msgReplicate2.setKeyValueMap(map);
		msgReplicate2.setSenderPort(myPort);

		Log.d(TAG, "Sending replicate2 message to port: " + successorPort);
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgReplicate2);


		startClientTask(msgReplicate2);

		//return replicate1 reply back to main coordinator
		Message replicate1Reply = new Message();
		replicate1Reply.setMessageType("Replicate1Reply");
		replicate1Reply.setSenderPort(myPort);
		replicate1Reply.setKeyValue(map.get("key"));

		Log.d(TAG, "Inside handleReplicate1 done");

		return replicate1Reply;

	}


	private void handleReplicate2(HashMap<String,String> map){

		Log.d(TAG, "Inside handleReplicate2");

		while(recoveryComplete.equals("false")){}

		writeToFile(map.get("key"), map.get("value"));

		Log.d(TAG, "Inside handleReplicate2 done");

	}

}

