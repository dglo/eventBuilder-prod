package icecube.daq.eventBuilder.backend;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.io.DispatchException;
import icecube.daq.io.Dispatcher;

import icecube.daq.eventBuilder.EventBuilderSPcachePayloadOutputEngine;
import icecube.daq.eventBuilder.SPDataAnalysis;

import icecube.daq.eventBuilder.monitoring.BackEndMonitor;

import icecube.daq.eventbuilder.IReadoutDataPayload;

import icecube.daq.eventbuilder.impl.EventPayload_v2;
import icecube.daq.eventbuilder.impl.EventPayload_v2Factory;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.MasterPayloadFactory;
import icecube.daq.payload.PayloadRegistry;
import icecube.daq.payload.SourceIdRegistry;

import icecube.daq.payload.splicer.Payload;

import icecube.daq.reqFiller.RequestFiller;

import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.Splicer;

import icecube.daq.trigger.ITriggerRequestPayload;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * JVM memory statistics.
 */
class MemoryStatistics
{
    /** Memory size designators. */
    private static final String[] MEMORY_SUFFIX = { "", "K", "G", "T" };

    /** Keep pointer to runtime data. */
    private Runtime rt = Runtime.getRuntime();

    /**
     * Simple constructor.
     */
    public MemoryStatistics()
    {
    }

    /**
     * Flush unused data from garbage collector.
     */
    public void flushMemory()
    {
        rt.runFinalization();
        rt.gc();
    }

    /**
     * Format byte size as a string.
     *
     * @param bytes
     */
    private static String formatBytes(long bytes)
    {
        int sufIdx = 0;
        while (bytes > 1024 * 1024 && sufIdx < MEMORY_SUFFIX.length - 1) {
            bytes /= 1024;
            sufIdx++;
        }

        return Long.toString(bytes) + MEMORY_SUFFIX[sufIdx];
    }

    /**
     * Return description of current statistics.
     *
     * @return description of current statistics
     */
    public String toString()
    {
        long free = rt.freeMemory();
        long total = rt.totalMemory();

        return (formatBytes(total - free) + " used, " +
                formatBytes(free) + " of " +
                formatBytes(total) + " free.");
    }
}

/**
 * Pull trigger requests from the front-end queue and readout data
 * from the splicer queue, and use those inputs to build events and send
 * them to DAQ dispatch.
 */
public class EventBuilderBackEnd
    extends RequestFiller
    implements BackEndMonitor, SPDataProcessor
{
    /** Message logger. */
    private static final Log LOG =
        LogFactory.getLog(EventBuilderBackEnd.class);

    /** Event builder source ID. */
    private static final ISourceID ME =
        SourceIdRegistry.getISourceIDFromNameAndId
        (DAQCmdInterface.DAQ_EVENTBUILDER, 0);

    private IByteBufferCache cacheManager;

    private Splicer splicer;
    private SPDataAnalysis analysis;
    private Dispatcher dispatcher;

    private EventBuilderSPcachePayloadOutputEngine cacheOutputEngine;

    // Factory to make EventPayloads.
    private EventPayload_v2Factory eventFactory;

    /** list of payloads to be deleted after back end has stopped */
    private ArrayList finalData;

    // The starting and ending times for the current request
    private long reqStartTime;
    private long reqEndTime;

    // per-run monitoring counters
    private long cumDispTime;
    private long curDispTime;
    private int execListLen;
    private int execListMax;
    private long maxDispSize;
    private long maxDispTime;
    private long numDispTimes;
    private int numExecuteCalls;
    private long numRecycled;
    private int numTruncateCalls;

    // lifetime monitoring counters
    private long prevRunTotalEvents;
    private long totStopsSent;

    /** Current run number. */
    private int runNumber;
    /** Have we reported a bad run number yet? */
    private boolean reportedBadRunNumber;
    /** Has the back end been reset? */
    private boolean isReset;

    private MemoryStatistics memStats = new MemoryStatistics();

    /**
     * Constructor
     *
     * @param master master payload factory
     * @param cacheManager buffer cache manager
     * @param splicer data splicer
     * @param analysis data splicer analysis
     * @param dispatcher DAQ dispatch
     */
    public EventBuilderBackEnd(MasterPayloadFactory master,
                               IByteBufferCache cacheManager,
                               Splicer splicer,
                               SPDataAnalysis analysis,
                               Dispatcher dispatcher)
    {
        super("EventBuilderBackEnd", true);

        if (dispatcher == null) {
            if (LOG.isFatalEnabled()) {
                try {
                    throw new NullPointerException("Dispatcher");
                } catch (NullPointerException ex) {
                    LOG.fatal("Constructor called with null dispatcher", ex);
                }
            }
            System.exit(1);
        }

        this.dispatcher = dispatcher;
        this.splicer = splicer;
        this.analysis = analysis;
        this.cacheManager = cacheManager;

        // register this object with splicer analysis
        analysis.setDataProcessor(this);

        //get factory object for event payloads
        eventFactory = new EventPayload_v2Factory();
    }

    /**
     * Increment the count of splicer.execute() calls.
     */
    public void addExecuteCall()
    {
        numExecuteCalls++;
    }

    /**
     * Add data received while splicer is stopping.
     *
     * @param coll collection of final data payloads
     */
    public void addFinalData(Collection coll)
    {
        if (finalData == null) {
            finalData = new ArrayList();
        } else if (LOG.isWarnEnabled()) {
            LOG.warn("Found existing list of final data");
        }

        finalData.addAll(coll);
    }

    /**
     * Increment the count of splicer.truncate() calls.
     */
    public void addTruncateCall()
    {
        numTruncateCalls++;
    }

    /**
     * Do a simple comparison of the request and data payloads.
     *
     * @param reqPayload request payload
     * @param dataPayload data payload
     *
     * @return <tt>-1</tt> if data is "before" request
     *         <tt>0</tt> if data is "within" request
     *         <tt>1</tt> if data is "later than" request
     */
    public int compareRequestAndData(IPayload reqPayload, IPayload dataPayload)
    {
        ITriggerRequestPayload req = (ITriggerRequestPayload) reqPayload;
        IReadoutDataPayload data = (IReadoutDataPayload) dataPayload;

        final int uid;
        if (data == null) {
            uid = Integer.MAX_VALUE;
        } else {
            uid = data.getRequestUID();
        }

        if (uid < req.getUID()) {
            return -1;
        } else if (uid == req.getUID()) {
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * Mark data boundary between runs.
     *
     * @param message run message
     *
     * @throws DispatchException if there is a problem changing the run
     */
    public void dataBoundary(String message)
        throws DispatchException
    {
        dispatcher.dataBoundary(message);
    }

    /**
     * Dispose of a data payload which is no longer needed.
     *
     * @param data payload
     */
    public void disposeData(IPayload data)
    {
        splicer.truncate((Spliceable) data);
    }

    /**
     * Dispose of a list of data payloads which are no longer needed.
     *
     * @param dataList list of data payload
     */
    public void disposeDataList(List dataList)
    {
        // if we truncate the final data payload,
        // all the others will also be removed
        Object obj = dataList.get(dataList.size() - 1);
        splicer.truncate((Spliceable) obj);
    }

    /**
     * Finish any tasks to be done just before the thread exits.
     */
    public void finishThreadCleanup()
    {
        analysis.stopDispatcher();
        cacheOutputEngine.sendLastAndStop();
        totStopsSent++;
    }

    /**
     * Get average millisecond time to dispatch event for this run.
     *
     * @return average dispatch time
     */
    public long getAverageDispatchTime()
    {
        if (numDispTimes == 0) {
            return 0;
        }

        return cumDispTime / numDispTimes;
    }

    /**
     * Get average number of readouts per event.
     *
     * @return readouts/event
     */
    public long getAverageReadoutsPerEvent()
    {
        return getAverageOutputDataPayloads();
    }

    /**
     * Get most recent millisecond time to dispatch event for this run.
     *
     * @return most recent dispatch time
     */
    public long getCurrentDispatchTime()
    {
        return curDispTime;
    }

    /**
     * Get ending time for event being built.
     *
     * @return end time
     */
    public long getCurrentEventEndTime()
    {
        return reqEndTime;
    }

    /**
     * Get start time for event being built.
     *
     * @return start time
     */
    public long getCurrentEventStartTime()
    {
        return reqStartTime;
    }

    /**
     * Get most recent splicer.execute() list length for this run.
     *
     * @return most recent splicer.execute() list length
     */
    public long getCurrentExecuteListLength()
    {
        return execListLen;
    }

    /**
     * Get current rate of events per second.
     *
     * @return events/second
     */
    public double getEventsPerSecond()
    {
        return getOutputsPerSecond();
    }

    /**
     * Get maximum millisecond time to dispatch event for this run.
     *
     * @return maximum dispatch time
     */
    public long getMaximumDispatchTime()
    {
        return maxDispTime;
    }

    /**
     * Get maximum splicer.execute() list length for this run.
     *
     * @return maximum splicer.execute() list length
     */
    public long getMaximumExecuteListLength()
    {
        return execListMax;
    }

    /**
     * Get memory statistics.
     *
     * @return memory statistics
     */
    public String getMemoryStatistics()
    {
        return memStats.toString();
    }

    /**
     * Get number of readouts which could not be loaded.
     *
     * @return number of bad readouts received
     */
    public long getNumBadReadouts()
    {
        return getNumBadDataPayloads();
    }

    /**
     * Number of trigger requests which could not be loaded.
     *
     * @return number of bad trigger requests
     */
    public long getNumBadTriggerRequests()
    {
        return getNumBadRequests();
    }

    /**
     * Get the number of dispatch times accumulated for this run.
     *
     * @return number of dispatch times
     */
    public long getNumDispatchTimes()
    {
        return numDispTimes;
    }

    /**
     * Get number of events which could not be sent.
     *
     * @return number of failed events
     */
    public long getNumEventsFailed()
    {
        return getNumOutputsFailed();
    }

    /**
     * Get number of empty events which were ignored.
     *
     * @return number of ignored events
     */
    public long getNumEventsIgnored()
    {
        return getNumOutputsIgnored();
    }

    /**
     * Get number of events sent.
     *
     * @return number of events sent
     */
    public long getNumEventsSent()
    {
        return getNumOutputsSent();
    }

    /**
     * Get number of calls to SPDataAnalysis.execute().
     *
     * @return number of execute() calls
     */
    public int getNumExecuteCalls()
    {
        return numExecuteCalls;
    }

    /**
     * Get number of readouts cached for event being built
     *
     * @return number of cached readouts
     */
    public int getNumReadoutsCached()
    {
        return getNumDataPayloadsCached();
    }

    /**
     * Get number of readouts thrown away.
     *
     * @return number of readouts thrown away
     */
    public long getNumReadoutsDiscarded()
    {
        return getNumDataPayloadsDiscarded();
    }

    /**
     * Get number of readouts queued for processing.
     *
     * @return number of readouts queued
     */
    public int getNumReadoutsQueued()
    {
        return getNumDataPayloadsQueued();
    }

    /**
     * Get number of readouts received.
     *
     * @return number of readouts received
     */
    public long getNumReadoutsReceived()
    {
        return getNumDataPayloadsReceived();
    }

    /**
     * Get number of events which could not be created.
     *
     * @return number of null events
     */
    public long getNumNullEvents()
    {
        return getNumNullOutputs();
    }

    /**
     * Get number of null readouts received.
     *
     * @return number of null readouts received
     */
    public long getNumNullReadouts()
    {
        return getNumNullDataPayloads();
    }

    /**
     * Get number of recycled payloads.
     *
     * @return number of recycled payloads
     */
    public long getNumRecycled()
    {
        return numRecycled;
    }

    /**
     * Number of trigger requests dropped while stopping.
     *
     * @return number of trigger requests dropped
     */
    public long getNumTriggerRequestsDropped()
    {
        return getNumRequestsDropped();
    }

    /**
     * Get number of trigger requests queued for the back end.
     *
     * @return number of trigger requests queued for the back end
     */
    public int getNumTriggerRequestsQueued() {
        return getNumRequestsQueued();
    }

    /**
     * Get number of trigger requests received from the global trigger
     * for this run.
     *
     * @return number of trigger requests received for this run
     */
    public long getNumTriggerRequestsReceived() {
        return getNumRequestsReceived();
    }

    /**
     * Get number of calls to SPDataAnalysis.truncate().
     *
     * @return number of truncate() calls
     */
    public int getNumTruncateCalls()
    {
        return numTruncateCalls;
    }

    /**
     * Get number of readouts not used for an event.
     *
     * @return number of unused readouts
     */
    public long getNumUnusedReadouts()
    {
        return getNumUnusedDataPayloads();
    }

    /**
     * Get the total number of events from the previous run.
     *
     * @return total number of events sent during the previous run
     */
    public long getPreviousRunTotalEvents()
    {
        return prevRunTotalEvents;
    }

    /**
     * Get current rate of readouts per second.
     *
     * @return readouts/second
     */
    public double getReadoutsPerSecond()
    {
        return getDataPayloadsPerSecond();
    }

    /**
     * Get size of event at maximum millisecond time for this run.
     *
     * @return size of event at maximum dispatch time
     */
    public long getSizeOfMaximumDispatchTime()
    {
        return maxDispSize;
    }

    /**
     * Get current rate of trigger requests per second.
     *
     * @return trigger requests/second
     */
    public double getTriggerRequestsPerSecond()
    {
        return getRequestsPerSecond();
    }

    /**
     * Get total number of readouts which could not be loaded since last reset.
     *
     * @return total number of bad readouts since last reset
     */
    public long getTotalBadReadouts()
    {
        return getTotalBadDataPayloads();
    }

    /**
     * Get the total dispatch time accumulated for this run.
     *
     * @return total dispatch time
     */
    public long getTotalDispatchTime()
    {
        return cumDispTime;
    }

    /**
     * Total number of events since last reset which could not be sent.
     *
     * @return total number of failed events
     */
    public long getTotalEventsFailed()
    {
        return getTotalOutputsFailed();
    }

    /**
     * Total number of empty events which were ignored since last reset.
     *
     * @return total number of ignored events
     */
    public long getTotalEventsIgnored()
    {
        return getTotalOutputsIgnored();
    }

    /**
     * Total number of events sent since last reset.
     *
     * @return total number of events sent since last reset.
     */
    public long getTotalEventsSent()
    {
        return getTotalOutputsSent();
    }

    /**
     * Total number of readouts thrown away since last reset.
     *
     * @return total number of readouts thrown away since last reset
     */
    public long getTotalReadoutsDiscarded()
    {
        return getTotalDataPayloadsDiscarded();
    }

    /**
     * Total number of readouts received since last reset.
     *
     * @return total number of readouts received since last reset
     */
    public long getTotalReadoutsReceived()
    {
        return getTotalDataPayloadsReceived();
    }

    /**
     * Total number of stop messages received from the splicer.
     *
     * @return total number of received stop messages
     */
    public long getTotalSplicerStopsReceived()
    {
        return getTotalDataStopsReceived();
    }

    /**
     * Get total number of trigger requests received from the global trigger
     * since the program began executing.
     *
     * @return total number of trigger requests received
     */
    public long getTotalTriggerRequestsReceived() {
        return getTotalRequestsReceived();
    }

    /**
     * Total number of stop messages received from the global trigger.
     *
     * @return total number of received stop messages
     */
    public long getTotalTriggerStopsReceived()
    {
        return getTotalRequestStopsReceived();
    }

    /**
     * Total number of stop messages sent to the string processors
     *
     * @return total number of sent stop messages
     */
    public long getTotalStopsSent()
    {
        return totStopsSent;
    }

    /**
     * Does this data payload match one of the request criteria?
     *
     * @param reqPayload request
     * @param dataPayload data
     *
     * @return <tt>true</tt> if the data payload is part of the
     *         current request
     */
    public boolean isRequested(IPayload reqPayload, IPayload dataPayload)
    {
        return true;
    }

    /**
     * Make an EventPayload out of the request in req and the data
     * in dataList.
     *
     * @param reqPayload trigger request
     * @param dataList list of readouts matching the trigger request
     *
     * @return The EventPayload created for the current TriggerRequest.
     */
    public IPayload makeDataPayload(IPayload reqPayload, List dataList)
    {
        // remember that we need to be reset
        isReset = false;

        if (reqPayload == null) {
            try {
                throw new NullPointerException("No request");
            } catch (Exception ex) {
                LOG.error("No current request; cannot send data", ex);
            }

            return null;
        }

        if (runNumber < 0 && !reportedBadRunNumber) {
            LOG.error("Run number has not yet been set");
            reportedBadRunNumber = true;
            return null;
        }

        ITriggerRequestPayload req = (ITriggerRequestPayload) reqPayload;

        IUTCTime startTime = req.getFirstTimeUTC();
        IUTCTime endTime = req.getLastTimeUTC();

        if (startTime == null || endTime == null) {
            LOG.error("Request may have been recycled; cannot send data");
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing Event " + startTime.getUTCTimeAsLong() +
                      " - " + endTime.getUTCTimeAsLong());
        }
        if (LOG.isWarnEnabled() && dataList.size() == 0) {
            LOG.warn("Sending empty event for window [" +
                     startTime.getUTCTimeAsLong() + " - " +
                     endTime.getUTCTimeAsLong() + "]");
        }

        final int eventType = req.getTriggerType();
        final int configId = req.getTriggerConfigID();

        Payload event =
            eventFactory.createPayload(req.getUID(), ME, startTime, endTime,
                                       eventType, configId, runNumber, req,
                                       new Vector(dataList));

        return event;
    }

    /**
     * Recycle all payloads in the list.
     *
     * @param payloadList list of payloads
     */
    public void recycleAll(Collection payloadList)
    {
        Iterator iter = payloadList.iterator();
        while (iter.hasNext()) {
            Payload payload = (Payload) iter.next();
            payload.recycle();
            numRecycled++;
        }
    }

    /**
     * Recycle payloads left after the final event.
     */
    public void recycleFinalData()
    {
        if (finalData != null) {
            recycleAll(finalData);

            // delete list once everything's been recycled
            finalData = null;
        }
    }

    /**
     * Register the string processor cache-flush output engine.
     *
     * @param oe output engine
     */
    public void registerStringProcCacheOutputEngine
        (EventBuilderSPcachePayloadOutputEngine oe)
    {
        cacheOutputEngine = oe;
    }

    /**
     * Reset the back end after it has been stopped.
     */
    public void reset()
    {
        if (!isReset) {
            prevRunTotalEvents = getNumOutputsSent();

            recycleFinalData();

            cumDispTime = 0;
            curDispTime = 0;
            execListLen = 0;
            execListMax = 0;
            maxDispSize = 0;
            maxDispTime = 0;
            numDispTimes = 0;
            numExecuteCalls = 0;
            numRecycled = 0;
            numTruncateCalls = 0;

            runNumber = Integer.MIN_VALUE;
            reportedBadRunNumber = false;

            isReset = true;
        }

        super.reset();
    }

    /**
     * Send an output payload.
     *
     * @param output payload being sent
     *
     * @return <tt>true</tt> if event was sent
     */
    public boolean sendOutput(IPayload output)
    {
        boolean sent = sendToDaqDispatch(output);

        // XXX not sending flush msg to string processors

        return sent;
    }

    /**
     * Send an event to DAQ Dispatch.
     *
     * @param event event being sent
     *
     * @return <tt>true</tt> if event was sent
     */
    private boolean sendToDaqDispatch(IPayload event)
    {
        EventPayload_v2 tmpEvent = (EventPayload_v2) event;

        final int payloadLen = tmpEvent.getPayloadLength();

        boolean sendPayload = false;
        boolean eventSent = false;

        ByteBuffer buffer = cacheManager.acquireBuffer(payloadLen);
        if (buffer == null) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot get buffer");
            }
        } else {
            buffer.limit(payloadLen);

            try {
                tmpEvent.writePayload(0, buffer);
                sendPayload = true;
            } catch (IOException ioe) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Could not write event to buffer", ioe);
                }
            }
        }

        if (sendPayload) {
            final long startTime = System.currentTimeMillis();

            buffer.position(0);
            try {
                dispatcher.dispatchEvent(buffer);

                if (LOG.isDebugEnabled()) {
                    IUTCTime utc;

                    utc = tmpEvent.getFirstTimeUTC();
                    long firstTime = (utc == null ? Long.MIN_VALUE :
                                      utc.getUTCTimeAsLong());

                    utc = tmpEvent.getLastTimeUTC();
                    long lastTime = (utc == null ? Long.MAX_VALUE :
                                     utc.getUTCTimeAsLong());

                    LOG.debug("Event " + firstTime + "-" + lastTime +
                              " written to daq-dispatch");
                }

                eventSent = true;
            } catch (DispatchException ex) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Could not dispatch event", ex);
                }
            }

            curDispTime = System.currentTimeMillis() - startTime;

            numDispTimes++;
            cumDispTime += curDispTime;

            if (curDispTime > maxDispTime) {
                maxDispTime = curDispTime;
                maxDispSize = buffer.limit();
            }
        }

        if (buffer != null) {
            cacheManager.returnBuffer(buffer);
        }

        tmpEvent.recycle();

        return eventSent;
    }

    /**
     * Record the length of the list passed to splicedAnalysis.execute().
     *
     * @param execListLen list length
     */
    public void setExecuteListLength(int execListLen)
    {
        this.execListLen = execListLen;
        if (execListLen > execListMax) {
            execListMax = execListLen;
        }
    }

    /**
     * Set the start and end times from the current request.
     *
     * @param payload request payload
     */
    public void setRequestTimes(IPayload payload)
    {
        final ITriggerRequestPayload req = (ITriggerRequestPayload) payload;

        reqStartTime =
            req.getFirstTimeUTC().getUTCTimeAsLong();
        reqEndTime =
            req.getLastTimeUTC().getUTCTimeAsLong();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Filling trigger#" + req.getUID() +
                      " [" + reqStartTime + "-" +
                      reqEndTime + "]");
        }
    }

    /**
     * Set the current run number.
     *
     * @param runNumber current run number
     */
    public void setRunNumber(int runNumber)
    {
        this.runNumber = runNumber;
    }

    /**
     * Inform back-end processor that the splicer has stopped.
     */
    public void splicerStopped()
    {
        addDataStop();
    }
}
