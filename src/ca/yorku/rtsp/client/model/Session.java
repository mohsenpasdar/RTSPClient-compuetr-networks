/*
 * Author: Jonatan Schroeder
 * Updated: March 2022
 *
 * This code may not be used without written consent of the authors.
 */

package ca.yorku.rtsp.client.model;

import ca.yorku.rtsp.client.exception.RTSPException;
import ca.yorku.rtsp.client.net.RTSPConnection;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.*;

/**
 * This class manages an open session with an RTSP server. It provides the main interaction between the network
 * interface and the user interface.
 */
public class Session {

    private Set<SessionListener> sessionListeners = new HashSet<SessionListener>();
    private RTSPConnection rtspConnection;
    private String videoName = null;

    // Constants for the rules
    private static final int PLAYBACK_INTERVAL = 40; // 25 frames per second
    private static final int PAUSE_THRESHOLD = 100; // 100 milliseconds
    private static final int RESUME_THRESHOLD = 80; // 80 milliseconds
    private static final int MIN_BuFFER_TO_PLAY = 50; // 50 frames

    // Sorted frame buffer
    private final TreeMap<Integer, Frame> frameBuffer = new TreeMap<>();

    // Scheduler for the playback of frames
    private final ScheduledExecutorService playbackScheduler =
            Executors.newSingleThreadScheduledExecutor();

    private ScheduledFuture<?> playbackTask;

    // Local playback state
    private boolean userRequestedPlay = false;
    private boolean sendingFramesToUI = false;

    // Client/server retrival state
    private boolean receeivingFromServer = false;

    // Sequence tracking and end-of-stream tracking
    private int nextSequenceToPlay = 0;
    private boolean endOfStreamReceived = false;
    private int endSequenceNumber = -1;

    /**
     * Creates a new RTSP session. This constructor will also create a new network connection with the server. No stream
     * setup is established at this point.
     *
     * @param server The IP address or host name of the RTSP server.
     * @param port   The port where the RTSP server is listening to.
     * @throws RTSPException If it was not possible to establish a connection with the server.
     */
    public Session(String server, int port) throws RTSPException {

        rtspConnection = new RTSPConnection(this, server, port);
    }

    /**
     * Adds a new listener interface to be called every time a session event (such as a change in video name or a new
     * frame) happens. Any interaction with user interfaces is done through these listeners.
     *
     * @param listener A SessionListener to be called when a session event happens.
     */
    public synchronized void addSessionListener(SessionListener listener) {
        sessionListeners.add(listener);
        listener.videoNameChanged(this.videoName);
    }

    /**
     * Removes an existing listener from the list of listeners to be called for session events.
     *
     * @param listener A SessionListener that should no longer be called when a session event happens.
     */
    public synchronized void removeSessionListener(SessionListener listener) {
        sessionListeners.remove(listener);
    }

    /**
     * Opens a new video file in the interface.
     *
     * @param videoName The name (URL) of the video to be opened. It should correspond to a local file in the server.
     */
    public synchronized void open(String videoName) {
        try {
            resetPlaybackState();

            rtspConnection.setup(videoName);
            this.videoName = videoName;

            for (SessionListener listener : sessionListeners)
                listener.videoNameChanged(this.videoName);

            rtspConnection.play();
            receeivingFromServer = true;

        } catch (RTSPException e) {
            listenerException(e);
        }
    }

    // Create a small helper to reset playback state
    private synchronized void resetPlaybackState() {
        if (playbackTask != null) {
            playbackTask.cancel(false);
            playbackTask = null;
        }

        frameBuffer.clear();
        nextSequenceToPlay = 0;
        endOfStreamReceived = false;
        endSequenceNumber = -1;
        sendingFramesToUI = false;
        receeivingFromServer = false;
        userRequestedPlay = false;
    }

    /**
     * Starts to play the existing file. It should only be called once a file has been opened. This function will return
     * immediately after the request was responded. Frames will be received in the background and will be handled by
     * the
     * <code>processReceivedFrame</code> method. If the video has been paused previously, playback will resume where it
     * stopped.
     */
    public synchronized void play() {
        try {
            rtspConnection.play();
        } catch (RTSPException e) {
            listenerException(e);
        }
    }

    /**
     * Pauses the playback the existing file. It should only be called once a file has started playing. This function
     * will return immediately after the request was responded. The server might still send a few frames before stopping
     * the playback completely.
     */
    public synchronized void pause() {
        try {
            rtspConnection.pause();
        } catch (RTSPException e) {
            listenerException(e);
        }
    }

    /**
     * Closes the currently open file. It should only be called once a file has been open.
     */
    public synchronized void close() {
        try {
            rtspConnection.teardown();
            processReceivedFrame(null);
            videoName = null;
            for (SessionListener listener : sessionListeners)
                listener.videoNameChanged(this.videoName);
        } catch (RTSPException e) {
            listenerException(e);
        }
    }

    private void listenerException(RTSPException e) {
        for (SessionListener listener : sessionListeners)
            listener.exceptionThrown(e);
    }

    /**
     * Closes the connection with the current server. This session element should not be used anymore after this point.
     */
    public synchronized void closeConnection() {
        rtspConnection.closeConnection();
    }

    /**
     * Processes a frame received from the RTSP server. This method
     * will direct the frame to the user interface to be processed and
     * presented to the user.
     *
     * @param frame The recently received frame.
     */
    public synchronized void processReceivedFrame(Frame frame) {
        if (videoName == null || frame == null) return;

        int seq = Short.toUnsignedInt(frame.getSequenceNumber());

        if (frameBuffer.isEmpty() && nextSequenceToPlay == 0) {
            nextSequenceToPlay = seq;
        }

        if (seq < nextSequenceToPlay) {
            // This frame is too old, ignore it
            return;
        }

        frameBuffer.put(seq, frame);

        if (frameBuffer.size() >= PAUSE_THRESHOLD && receeivingFromServer) {
            try {
                rtspConnection.pause();
                receeivingFromServer = false;
            } catch (RTSPException e) {
                listenerException(e);
            }
        }

        // Check if we can start/resume playback
        maybeStartPlayback();
    }

    // Maybe start local playback if conditions are met
    private synchronized void maybeStartPlayback() {
        // Check if the user has requested to play the video
        if (!userRequestedPlay) {
            return;
        }

        // Check if a video is open
        if (videoName == null) {
            return;
        }

        // Check if playback is already running
        if (sendingFramesToUI) {
            return;
        }

        // Check if we have enough frames to start playback
        boolean canStartNormally = frameBuffer.size() >= MIN_BuFFER_TO_PLAY;
        boolean canDrainAfterEnd = endOfStreamReceived && !frameBuffer.isEmpty();

        if (canStartNormally || canDrainAfterEnd) {
            sendingFramesToUI = true;
            startPlayBackTask();
        }
    }

    // Start playback task
    private synchronized void startPlayBackTask() {
        if (playbackTask != null && !playbackTask.isCancelled() && !playbackTask.isDone()) {
            return;
        }

        playbackTask = playbackScheduler.scheduleAtFixedRate(
                this::playbackTick,
                0,
                PLAYBACK_INTERVAL,
                TimeUnit.MILLISECONDS
        );
    }

    // Stop playback task
    private synchronized void stopPlayBackTask() {
        if (playbackTask != null) {
            playbackTask.cancel(false);
            playbackTask = null;
        }

        sendingFramesToUI = false;
    }

    private synchronized void playbackTick() {
        if (videoName == null || !userRequestedPlay || !sendingFramesToUI) {
            stopPlayBackTask();
            return;
        }

        // If playback is already fully finished, notify the UI and stop the task
        if (endOfStreamReceived && frameBuffer.isEmpty() && nextSequenceToPlay >= endSequenceNumber) {
            stopPlayBackTask();
            for (SessionListener listener : sessionListeners)
                listener.videoEnded();
            return;
        }

        // If the buffer is empty but the stream hasn't ended, stop local playback
        if (frameBuffer.isEmpty() && !endOfStreamReceived) {
            stopPlayBackTask();
            return;
        }
    }

    /**
     * Processes a notification received from the RTSP server that the
     * video ended. This method will direct the notification to the
     * user interface to be handled as needed.
     *
     * @param sequenceNumber The sequence number for the end
     * notification. Corresponds to the last frame plus one. Can be
     * used to identify a missing frame at the end of the stream.
     */
    public synchronized void videoEnded(int sequenceNumber) {
        for (SessionListener listener : sessionListeners)
            listener.videoEnded();
    }

    /**
     * Returns the name of the currently opened video.
     *
     * @return The name of the video currently open, or null if no video is open.
     */
    public synchronized String getVideoName() {
        return videoName;
    }
}
