/*
 * Author: Jonatan Schroeder
 * Updated: October 2022
 *
 * This code may not be used without written consent of the authors.
 */

package ca.yorku.rtsp.client.net;

import ca.yorku.rtsp.client.exception.RTSPException;
import ca.yorku.rtsp.client.model.Frame;
import ca.yorku.rtsp.client.model.Session;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class represents a connection with an RTSP server.
 */
public class RTSPConnection {

    private static final int BUFFER_LENGTH = 0x10000;
    private final Session session;

    // TODO Add additional fields, if necessary
    private Socket rtspSocket;
    private BufferedReader rtspReader;
    private BufferedWriter rtspWriter;
    private DatagramSocket rtpSocket;
    private RTPReceivingThread rtpReceivingThread;
    private String sessionId;
    private int cSeq;
    private String videoName;
    /**
     * Establishes a new connection with an RTSP server. No message is
     * sent at this point, and no stream is set up.
     *
     * @param session The Session object to be used for connectivity with the UI.
     * @param server  The hostname or IP address of the server.
     * @param port    The TCP port number where the server is listening to.
     * @throws RTSPException If the connection couldn't be accepted,
     *                       such as if the host name or port number
     *                       are invalid or there is no connectivity.
     */
    public RTSPConnection(Session session, String server, int port) throws RTSPException {

        this.session = session;

        // TODO
        // Initialize sequence number to 0
        cSeq = 0;
        try {
            // Create TCP socket connection to RTSP server
            rtspSocket = new Socket(server, port);
            // Create input reader for RTSP responses
            rtspReader = new BufferedReader(new InputStreamReader(rtspSocket.getInputStream()));
            // Create output writer for RTSP requests
            rtspWriter = new BufferedWriter(new OutputStreamWriter(rtspSocket.getOutputStream()));
        } catch (IOException e) {
            throw new RTSPException("Could not establish connection with the server: " + e.getMessage());
        }
    }

    /**
     * Sets up a new video stream with the server. This method is
     * responsible for sending the SETUP request, receiving the
     * response and retrieving the session identification to be used
     * in future messages. It is also responsible for establishing an
     * RTP datagram socket to be used for data transmission by the
     * server. The datagram socket should be created with a random
     * available UDP port number, and the port number used in that
     * connection has to be sent to the RTSP server for setup. This
     * datagram socket should also be defined to timeout after 2
     * seconds if no packet is received.
     *
     * @param videoName The name of the video to be setup.
     * @throws RTSPException If there was an error sending or
     *                       receiving the RTSP data, or if the RTP
     *                       socket could not be created, or if the
     *                       server did not return a successful
     *                       response.
     */
    public synchronized void setup(String videoName) throws RTSPException {

        // TODO
        this.videoName = videoName;
        try {
            // Create UDP socket for RTP stream reception
            rtpSocket = new DatagramSocket();
            // Set socket timeout to 2 seconds
            rtpSocket.setSoTimeout(2000);
        } catch (SocketException e) {
            throw new RTSPException("Could not create RTP socket: " + e.getMessage());
        }

        // Send SETUP request
        cSeq ++;
        String request = "SETUP " + videoName + " RTSP/1.0\r\n" +
                "CSeq: " + cSeq + "\r\n" +
                "Transport: RTP/UDP; client_port=" + rtpSocket.getLocalPort() + "\r\n\r\n";

        try {
            // Send request to server
            rtspWriter.write(request);
            rtspWriter.flush();

            // Read response from server
            RTSPResponse response = readRTSPResponse();

            if (response == null) {
                throw new RTSPException("Server closed RTSP connection unexpectedly.");
            }

            // Check if response indicates success
            if (response.getResponseCode() != 200) {
                rtpSocket .close();
                rtpSocket = null;
                throw new RTSPException("Failed to setup stream: " + response.getResponseMessage());
            }

            System.out.println("Received RTSP response: " + response.toString());
            // Extract session ID from response headers
            String sessionHeader = response.getHeaderValue("Session");
            if (sessionHeader == null) {
                throw new RTSPException("No Session header in SETUP response");
            }
            sessionId = sessionHeader.trim();

        } catch (IOException | RTSPException e) {
            if (rtpSocket != null && !rtpSocket.isClosed()) {
                rtpSocket.close();
                rtpSocket = null;
            }
            sessionId = null;
            this.videoName = null;
            throw (e instanceof RTSPException)
                    ? (RTSPException) e
                    : new RTSPException("Error sending SETUP request: " + e.getMessage());
        }
    }

    /**
     * Starts (or resumes) the playback of a set up stream. This
     * method is responsible for sending the request, receiving the
     * response and, in case of a successful response, starting a
     * separate thread responsible for receiving RTP packets with
     * frames (achieved by calling start() on a new object of type
     * RTPReceivingThread).
     *
     * @throws RTSPException If there was an error sending or
     *                       receiving the RTSP data, or if the server
     *                       did not return a successful response.
     */
    public synchronized void play() throws RTSPException {

        // TODO
        // Verify that stream is properly set up before playing
        if (sessionId == null || rtpSocket == null || rtpSocket.isClosed() || videoName == null) {
            throw new RTSPException("Stream is not set up. Please call setup() before play().");
        }

        // Send PLAY request
        cSeq ++;
        String request = "PLAY " + videoName + " RTSP/1.0\r\n" +
                "CSeq: " + cSeq + "\r\n" +
                "Session: " + sessionId + "\r\n\r\n";

        try {
            // Send request to server
            rtspWriter.write(request);
            rtspWriter.flush();

            // Read response from server
            RTSPResponse response = readRTSPResponse();
            if (response == null) {
                throw new RTSPException("Server closed RTSP connection unexpectedly.");
            }

            // Check if response indicates success
            if (response.getResponseCode() != 200) {
                throw new RTSPException("Failed to play stream: " + response.getResponseMessage());
            }

            System.out.println("Received RTSP response: " + response.toString());

            // Start RTP receiving thread to receive video frames
            if (rtpReceivingThread == null || !rtpReceivingThread.isAlive()) {
                rtpReceivingThread = new RTPReceivingThread();
                rtpReceivingThread.start();
            }
        } catch (IOException e) {
            throw new RTSPException("Error sending PLAY request: " + e.getMessage());
        }

    }

    private class RTPReceivingThread extends Thread {
        /**
         * Continuously receives RTP packets until the thread is
         * cancelled or until an RTP packet is received with a
         * zero-length payload. Each packet received from the datagram
         * socket is assumed to be no larger than BUFFER_LENGTH
         * bytes. This data is then parsed into a Frame object (using
         * the parseRTPPacket() method) and the method
         * session.processReceivedFrame() is called with the resulting
         * packet. The receiving process should be configured to
         * timeout if no RTP packet is received after two seconds. If
         * a frame with zero-length payload is received, indicating
         * the end of the stream, the method session.videoEnded() is
         * called, and the thread is terminated.
         */
        @Override
        public void run() {

            // TODO
            byte[] buffer = new byte[BUFFER_LENGTH];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            // Loop until thread is interrupted or socket is closed
            while (!Thread.currentThread().isInterrupted() && rtpSocket != null && !rtpSocket.isClosed()) {
                try {
                    // Reset packet length before each receive
                    packet.setLength(buffer.length);
                    // Wait for incoming RTP packet
                    rtpSocket.receive(packet);
                    // Parse RTP packet into Frame object
                    Frame frame = parseRTPPacket(packet);

                    // Check if this is the end-of-stream marker (zero-length payload)
                    if (frame.getPayloadLength() == 0) {
                        session.videoEnded(frame.getSequenceNumber());
                        break;
                    }

                    // Pass frame to session for processing
                    session.processReceivedFrame(frame);

                } catch (SocketTimeoutException e) {
                    // Timeout occurred, continue to wait for packets
                } catch (SocketException e) {
                    // Socket was closed, exit the thread
                    break;
                } catch (IOException e) {
                    // Other I/O error occurred, log and continue
                    System.err.println("Error receiving RTP packet: " + e.getMessage());
                    break;
                }
            }
        }
    }

    /**
     * Pauses the playback of a set up stream. This method is
     * responsible for sending the request, receiving the response
     * and, in case of a successful response, stopping the thread
     * responsible for receiving RTP packets with frames.
     *
     * @throws RTSPException If there was an error sending or
     *                       receiving the RTSP data, or if the server
     *                       did not return a successful response.
     */
    public synchronized void pause() throws RTSPException {

        // TODO
        // Verify that stream is properly set up before pausing
        if (sessionId == null || rtpSocket == null || rtpSocket.isClosed() || videoName == null) {
            throw new RTSPException("Stream is not set up. Please call setup() before pause().");
        }

        // Send PAUSE request
        cSeq ++;
        String request = "PAUSE " + videoName + " RTSP/1.0\r\n" +
                "CSeq: " + cSeq + "\r\n" +
                "Session: " + sessionId + "\r\n\r\n";

        try {
            // Send request to server
            rtspWriter.write(request);
            rtspWriter.flush();

            // Read response from server
            RTSPResponse response = readRTSPResponse();
            if (response == null) {
                throw new RTSPException("Server closed RTSP connection unexpectedly.");
            }

            // Check if response indicates success
            if (response.getResponseCode() != 200) {
                throw new RTSPException("Failed to pause stream: " + response.getResponseMessage());
            }

            System.out.println("Received RTSP response: " + response.toString());

            // Stop RTP receiving thread
            if (rtpReceivingThread != null && rtpReceivingThread.isAlive()) {
                rtpReceivingThread.interrupt();
                rtpReceivingThread = null;
            }
        } catch (IOException e) {
            throw new RTSPException("Error sending PAUSE request: " + e.getMessage());
        }
    }
            

    /**
     * Terminates a set up stream. This method is responsible for
     * sending the request, receiving the response and, in case of a
     * successful response, closing the RTP socket. This method does
     * not close the RTSP connection, and a further SETUP in the same
     * connection should be accepted. Also, this method can be called
     * both for a paused and for a playing stream, so the thread
     * responsible for receiving RTP packets will also be cancelled,
     * if active.
     *
     * @throws RTSPException If there was an error sending or
     *                       receiving the RTSP data, or if the server
     *                       did not return a successful response.
     */
    public synchronized void teardown() throws RTSPException {

        // TODO
        // Verify that stream is active
        if (sessionId == null || videoName == null || rtpSocket == null || rtpSocket.isClosed()) {
            throw new RTSPException("No active stream to tear down.");
        }

        // Send TEARDOWN request
        cSeq ++;
        String request = "TEARDOWN " + videoName + " RTSP/1.0\r\n" +
                "CSeq: " + cSeq + "\r\n" +
                "Session: " + sessionId + "\r\n\r\n";

        try {
            // Send request to server
            rtspWriter.write(request);
            rtspWriter.flush();

            // Read response from server
            RTSPResponse response = readRTSPResponse();
            if (response == null) {
                throw new RTSPException("Server closed RTSP connection unexpectedly.");
            }

            // Check if response indicates success
            if (response.getResponseCode() != 200) {
                throw new RTSPException("Failed to teardown stream: " + response.getResponseMessage());
            }

            System.out.println("Received RTSP response: " + response.toString());

            // Stop RTP receiving thread
            if (rtpReceivingThread != null && rtpReceivingThread.isAlive()) {
                rtpReceivingThread.interrupt();
            }

            // Close RTP socket
            if (rtpSocket != null && !rtpSocket.isClosed()) {
                rtpSocket.close();
                rtpSocket = null;
            }

            // Set the receiving thread to null
            rtpReceivingThread = null;

            // Clear session ID and video name
            sessionId = null;
            videoName = null;

        } catch (IOException e) {
            throw new RTSPException("Error sending TEARDOWN request: " + e.getMessage());
        }
    }

    /**
     * Closes the connection with the RTSP server. This method should
     * also close any open resource associated to this connection,
     * such as the RTP connection and thread, if it is still open.
     */
    public synchronized void closeConnection() {

        // TODO
        // Stop RTP receiving thread if active
        if (rtpReceivingThread != null && rtpReceivingThread.isAlive()) {
            rtpReceivingThread.interrupt();
        }

        // Close RTP socket
        if (rtpSocket != null && !rtpSocket.isClosed()) {
            rtpSocket.close();
            rtpSocket = null;
        }

        // Close RTSP writer
        try {
            rtspWriter.close();
        } catch (IOException e) {
            System.err.println("Error closing RTSP writer: " + e.getMessage());
        }

        // Close RTSP reader
        try {
            rtspReader.close();
        } catch (IOException e) {
            System.err.println("Error closing RTSP reader: " + e.getMessage());
        }

        // Close RTSP socket
        try {
            rtspSocket.close();
        } catch (IOException e) {
            System.err.println("Error closing RTSP socket: " + e.getMessage());
        }

        // Clear all resources and state
        rtspWriter = null;
        rtspReader = null;
        rtspSocket = null;
        sessionId = null;
        videoName = null;
    }

    /**
     * Parses an RTP packet into a Frame object. This method is
     * intended to be a helper method in this class, but it is made
     * public to facilitate testing.
     *
     * @param packet the byte representation of a frame, corresponding to the RTP
     *               packet.
     * @return A Frame object.
     */
    public static Frame parseRTPPacket(DatagramPacket packet) {

        // TODO
        byte[] data = packet.getData();
        int offset = packet.getOffset();
        int length = packet.getLength();

        // RTP packet must be at least 12 bytes (fixed header size)
        if (length < 12) {
            throw new IllegalArgumentException("Invalid RTP packet: packet too short.");
        }

        // Byte 1 contains: marker (1 bit), payload type (7 bits)
        byte secondByte = data[offset + 1];
        // Extract marker bit (most significant bit)
        boolean marker = (secondByte & 0x80) != 0;
        // Extract payload type (lower 7 bits)
        byte payloadType = (byte) (secondByte & 0x7F);

        // Bytes 2-3 contain the sequence number (16 bits)
        short sequenceNumber = ByteBuffer.wrap(data, offset + 2, 2).order(ByteOrder.BIG_ENDIAN).getShort();

        // Bytes 4-7 contain the timestamp (32 bits)
        int timestamp = ByteBuffer.wrap(data, offset + 4, 4).order(ByteOrder.BIG_ENDIAN).getInt();

        // Payload starts from byte 12 and continues to end of packet
        int payloadLength = length - 12;
        int payloadOffset = offset + 12;

        return new Frame(payloadType, marker, sequenceNumber, timestamp, data, payloadOffset, payloadLength);
    }


    /**
     * Reads and parses an RTSP response from the socket's input. This
     * method is intended to be a helper method in this class, but it
     * is made public to facilitate testing.
     *
     * @return An RTSPResponse object if the response was read
     *         completely, or null if the end of the stream was reached.
     * @throws IOException   In case of an I/O error, such as loss of connectivity.
     * @throws RTSPException If the response doesn't match the expected format.
     */
    public RTSPResponse readRTSPResponse() throws IOException, RTSPException {

        // TODO
        RTSPResponse response = null;

        // Read status line from server
        String statusLine = rtspReader.readLine();
        if (statusLine == null) {
            return null; // End of stream
        }

        // Parse status line format: "RTSP/1.0 <code> <message>"
        String[] statusParts = statusLine.split(" ", 3);
        if (statusParts.length < 3) {
            throw new RTSPException("Invalid RTSP response status line: " + statusLine);
        }

        String rtspVersion = statusParts[0];
        int responseCode = Integer.parseInt(statusParts[1]);
        String responseMessage = statusParts[2];

        // Create response object with status line data
        response = new RTSPResponse(rtspVersion, responseCode, responseMessage);

        // Read and parse header lines until empty line
        String headerLine;
        while ((headerLine = rtspReader.readLine()) != null && !headerLine.isEmpty()) {
            // Parse header format: "<name>: <value>"
            String[] headerParts = headerLine.split(": ", 2);
            if (headerParts.length == 2) {
                response.addHeaderValue(headerParts[0], headerParts[1]);
            }
        }

        return response;
    }
}
