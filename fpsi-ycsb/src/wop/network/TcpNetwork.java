package wop.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;

import wcc.common.KillOnExceptionHandler;
import wop.messages.Message;
import wop.messages.MessageFactory;


public class TcpNetwork extends Network implements Runnable {
	private final TcpConnection[] connections;
	private final ServerSocket server;
	private final Thread acceptorThread;
	private final int id;
	private boolean started = false;

	/**
	 * Creates new network for handling connections with other n.
	 *
	 * @throws IOException if opening server socket fails
	 */
	public TcpNetwork(int id) throws IOException {
		this.id = id;
		this.connections = new TcpConnection[p.numReplicas];

		int port = p.getLocalProcess().getReplicaPort() + (id * 100);
		this.server = new ServerSocket();
		server.bind(new InetSocketAddress((InetAddress) null, port));
		this.acceptorThread = new Thread(this, "TcpNetwork");
		acceptorThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
	}

	@Override
	public void start() {
		if (!started) {
			for (int i = 0; i < connections.length; i++) {
				if (i < p.localId) {
					connections[i] = new TcpConnection(this,p.config.getProcess(i), id, false);
					connections[i].start();
				}
				if (i > p.localId) {
					connections[i] = new TcpConnection(this, p.config.getProcess(i), id, true);
					connections[i].start();
				}
			}
			// Start the thread that listens and accepts new connections.
			// Must be started after the connections are initialized (code
			// above)
			acceptorThread.start();
			started = true;
		}
	}

	/**
	 * Sends binary data to specified destination.
	 *
	 * @param message - binary data to send
	 * @param destination - id of replica to send data to
	 * @return true if message was sent; false if some error occurred
	 */
	public boolean send(byte[] message, int destination) {
		assert destination != p.localId;
		return connections[destination].send(message);
	}

	@Override
	public void sendMessage(Message message, BitSet destinations) {
		assert !destinations.isEmpty() : "Sending a message to no one";

		byte[] bytes = message.toByteArray();
		for (int i = destinations.nextSetBit(0); i >= 0; i = destinations.nextSetBit(i + 1)) {
			if (i == p.localId) {
				// do not send message to self (just fire event)
				fireReceiveMessage(MessageFactory.create(new DataInputStream(new ByteArrayInputStream(bytes))), p.localId);
			} else {
				send(bytes, i);
			}
		}

		// Not really sent, only queued for sending,
		// but it's good enough for the notification
		fireSentMessage(message, destinations);
	}

	/**
	 * Main loop which accepts incoming connections.
	 */
	public void run() {
		while (!Thread.interrupted()) {
			try {
				Socket socket = server.accept();
				initializeConnection(socket);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void initializeConnection(Socket socket) {
		try {
			socket.setTcpNoDelay(true);
			DataInputStream input = new DataInputStream(
					new BufferedInputStream(socket.getInputStream()));
			DataOutputStream output = new DataOutputStream(
					new BufferedOutputStream(socket.getOutputStream()));
			int replicaId = input.readInt();
			if (replicaId < 0 || replicaId >= p.numReplicas) {
				socket.close();
				return;
			}
			if (replicaId == p.localId) {
				socket.close();
				return;
			}

			connections[replicaId].setConnection(socket, input, output);
		} catch (IOException e) {
			try {
				socket.close();
			} catch (IOException e1) {
			}
		}
	}

	public void closeAll() {
		for (TcpConnection c : connections) {
			try {
				if (c != null) c.stop();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean isStarted() {
		return started;
	}
}
