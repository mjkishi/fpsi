package wop.network;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import wcc.common.KillOnExceptionHandler;
import wcc.common.PID;
import wcc.common.ProcessDescriptor;
import wop.messages.Message;
import wop.messages.MessageFactory;



/**
 * This class is responsible for handling stable TCP connection to other
 * replica, provides two methods for establishing new connection: active and
 * passive. In active mode we try to connect to other side creating new socket
 * and connects. If passive mode is enabled, then we wait for socket from the
 * <code>SocketServer</code> provided by <code>TcpNetwork</code>.
 * <p>
 * Every time new message is received from this connection, it is deserialized,
 * and then all registered network listeners in related <code>TcpNetwork</code>
 * are notified about it.
 *
 * @see TcpNetwork
 */
public class TcpConnection {
	private final PID replica;
	/** true if connection should be started by this replica; */
	private final boolean active;
	private final int id;
	private final TcpNetwork network;
	private final Thread senderThread;
	private final Thread receiverThread;
	private final BlockingQueue<byte[]> sendQueue = new ArrayBlockingQueue<byte[]>(1000000);
	private Socket socket;
	private DataInputStream input;
	private OutputStream output;
	private volatile boolean connected = false;
	private int dropped = 0;
	private int droppedFull = 0;

	/**
	 * Creates a new TCP connection to specified replica.
	 *
	 * @param network - related <code>TcpNetwork</code>.
	 * @param replica - replica to connect to.
	 * @param active - initiates connection if true; waits for remote connection
	 *            otherwise.
	 */
	public TcpConnection(TcpNetwork network, PID replica, int id, boolean active) {
		this.network = network;
		this.replica = replica;
		this.id = id;
		this.active = active;
		this.receiverThread = new Thread(new ReceiverThread(), "ReplicaIORcv-" + this.replica.getId());
		this.senderThread = new Thread(new Sender(), "ReplicaIOSnd-" + this.replica.getId());
		receiverThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
		senderThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());               
	}
	/**
	 * Starts the receiver and sender thread.
	 */
	public synchronized void start() {
		receiverThread.start();
		senderThread.start();
	}

	/**
	 * Sends specified binary packet using underlying TCP connection.
	 *
	 * @param message - binary packet to send
	 * @return true if sending message was successful
	 */
	public boolean send(byte[] message) {
		try {
			if (connected)  {
				sendQueue.put(message);
			} else {
				if (dropped % 102400 == 0) {
				}
				dropped++;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
		return true;
	}
	/**
	 * Registers new socket to this TCP connection. Specified socket should be
	 * initialized connection with other replica. First method tries to close
	 * old connection and then set-up new one.
	 *
	 * @param socket - active socket connection
	 * @param input - input stream from this socket
	 * @param output - output stream from this socket
	 */
	public synchronized void setConnection(Socket socket, DataInputStream input,
			DataOutputStream output) {
		assert socket.isConnected() : "Invalid socket state";
		close();
		// initialize new connection
		this.socket = socket;
		this.input = input;
		this.output = output;
		connected = true;
		// if main thread wait for this connection notifyClient it
		notifyAll();
	}

	/**
	 * Stops current connection and stops all underlying threads.
	 *
	 * Note: This method waits until all threads are finished.
	 *
	 * @throws InterruptedException
	 */
	public void stop() throws InterruptedException {
		close();
		receiverThread.interrupt();
		senderThread.interrupt();

		receiverThread.join();
		senderThread.join();
	}

	/**
	 * Establishes connection to host specified by this object. If this is
	 * active connection then it will try to connect to other side. Otherwise we
	 * will wait until connection will be set-up using
	 * <code>setConnection</code> method. This method will return only if the
	 * connection is established and initialized properly.
	 *
	 * @throws InterruptedException
	 */
	private void connect() throws InterruptedException {
		if (active) {
			// this is active connection so we try to connect to host
			while (true) {
				try {
					socket = new Socket();
					socket.setTcpNoDelay(true);
					try {
						InetSocketAddress s1=new InetSocketAddress(replica.getHostname(),
								replica.getReplicaPort() + id * 100);
						socket.connect(s1);
						InetAddress clientAddress=s1.getAddress();      
					} catch (ConnectException e) {
						Thread.sleep(ProcessDescriptor.getInstance().tcpReconnectTimeout);
						continue;
					}

					input = new DataInputStream(
							new BufferedInputStream(socket.getInputStream()));
					output = socket.getOutputStream();
					int v = ProcessDescriptor.getInstance().localId;
					output.write((v >>> 24) & 0xFF);
					output.write((v >>> 16) & 0xFF);
					output.write((v >>>  8) & 0xFF);
					output.write(v & 0xFF);
					output.flush();
					// connection established
					break;
				} catch (IOException e) {
					// some other problem (possibly other side closes
					// connection while initializing connection); for debug
					// purpose we print this message
					long sleepTime = ProcessDescriptor.getInstance().tcpReconnectTimeout;
					Thread.sleep(sleepTime);
				}
			}
			// Wake up the sender thread
			synchronized (this) {     	
				connected = true;
				notifyAll();           
			}
		} else {
			// this is passive connection so we are waiting until other replica
			// connect to us; we will be notified by setConnection method       	
			//  System.out.println(replica.getReplicaPort() + id * 100+ " is not active yet");
			synchronized (this) {
				while (!connected) {
					//  	int last=replica.getReplicaPort() ;
					wait();                         
					//   System.out.println(replica.getReplicaPort() + id * 100+ " is now active");                    
					if(ProcessDescriptor.getInstance().getLocalProcess().getId()==ProcessDescriptor.getInstance().numReplicas-1 /*&& last==ProcessDescriptor.getInstance().numReplicas-2*/) {
						//  	System.out.println("All the nodes are successfully connected!" );
					}
				}
			}
		}
	}

	/**
	 * Closes the connection.
	 */
	private synchronized void close() {
		connected = false;
		if (socket != null && socket.isConnected()) {
			try {
				socket.shutdownOutput();
				socket.close();
				socket = null;
			} catch (IOException e) {
			}
		}
	}

	private final class Sender implements Runnable {
		public void run() {
			try {
				while (!Thread.interrupted()) {
					byte[] msg = sendQueue.take();
					// ignore message if not connected
					// Works without memory barrier because connected is volatile
					if (!connected) {
						continue;
					}
					try {
						output.write(msg);
						output.flush();
					} catch (IOException e) {
						close();
					}
				}
			} catch (InterruptedException e) {
			}
		}
	}

	/**
	 * Main loop used to connect and read from the socket.
	 */
	private final class ReceiverThread implements Runnable {
		public void run() {
			while (true) {
				try {
					connect();                    
				} catch (InterruptedException e) {
					break;
				}
				while (true) {
					if (Thread.interrupted()) {
						close();
						return;
					}
					try {
						Message message = MessageFactory.create(input);
						network.fireReceiveMessage(message, replica.getId());

					} catch (Exception e) {
						// end of stream or problem with socket occurred so
						// close connection and try to establish it again
						close();
						break;
					}
				}
			}
		}
	}
}
