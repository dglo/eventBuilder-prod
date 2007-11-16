package icecube.daq.eventBuilder.backend;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.io.DispatchException;
import icecube.daq.io.Dispatcher;

import icecube.daq.eventBuilder.EventBuilderSPcachePayloadOutputEngine;
import icecube.daq.eventBuilder.SPDataAnalysis;

import icecube.daq.eventBuilder.monitoring.BackEndMonitor;

import icecube.daq.eventbuilder.IReadoutDataPayload;

import icecube.daq.eventbuilder.impl.EventPayload_v3;
import icecube.daq.eventbuilder.impl.EventPayload_v3Factory;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ILoadablePayload;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Pull trigger requests from the front-end queue and readout data
 * from the splicer queue, and use those inputs to build events and send
 * them to DAQ dispatch.
 */
public class EventBuilderBackEnd
    extends RequestFiller
    implements BackEndMonitor, SPDataProcessor
{
    /**
     * Subrun event counts.
     */
    class SubrunEventCount
        implements Comparable
    {
        /** subrun number */
        private int num;
        /** number of events in subrun */
        private long count;

        /**
         * Create a subrun event count
         *
         * @param num subrun number
         * @param count number of events
         */
        SubrunEventCount(int num, long count)
        {
            this.num = num;
            this.count = count;
        }

        /**
         * Compare this object to another.
         *
         * @param obj compared cobject
         *
         * @return the usual comparison results
         */
        public int compareTo(Object obj)
        {
            if (obj == null) {
                return -1;
            }

            if (!(obj instanceof SubrunEventCount)) {
                return getClass().getName().compareTo(obj.getClass().getName());
            }

            SubrunEventCount sd = (SubrunEventCount) obj;

            if (num == sd.num) {
                return 0;
            }

            if (num == -sd.num) {
                if (num < 0) {
                    return -1;
                }

                return 1;
            }

            int absNum = (num < 0 ? -num : num);
            int absSDNum = (sd.num < 0 ? -sd.num : sd.num);
            return absNum - absSDNum;
        }

        /**
         * Is this object equal to another object?
         *
         * @param obj compared object
         *
         * @return <tt>true</tt> if the compared object is equal to this object
         */
        public boolean equals(Object obj)
        {
            return compareTo(obj) == 0;
        }

        /**
         * Get the number of events in this subrun
         *
         * @return number of events
         */
        long getCount()
        {
            return count;
        }

        /**
         * Get the hash code for this object
         *
         * @return subrun number
         */
        public int hashCode()
        {
            return num;
        }
    }

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
    private EventPayload_v3Factory eventFactory;

    /** list of payloads to be deleted after back end has stopped */
    private ArrayList finalData;

    // per-run monitoring counters
    private int execListLen;

    // lifetime monitoring counters
    private long prevRunTotalEvents;
    private long totStopsSent;

    /** Current run number. */
    private int runNumber;
    /** Have we reported a bad run number yet? */
    private boolean reportedBadRunNumber;

    /** Current subrun number. */
    private int subrunNumber;
    /** Current subrun start time. */
    private long subrunStart;
    /** Number of events in current subrun. */
    private long subrunEventCount;
    /** If we are transitioning into a new subrun (prepareSubrun() has
     * been called and not yet acted upon) */
    private boolean subrunTransition;
    /** If we have a new subrunStart value which has not yet been
     * reached (commitSubrun() has been called but not yet acted upon) */
    private boolean newSubrunStartTime;
    /** List of all subrun event counts. */
    private HashMap<SubrunEventCount, SubrunEventCount> subrunEventCountMap =
        new HashMap<SubrunEventCount, SubrunEventCount>();

    /** Has the back end been reset? */
    private boolean isReset;

    /**
     * Constructor
     *
     * @param eventCache event buffer cache manager
     * @param splicer data splicer
     * @param analysis data splicer analysis
     * @param dispatcher DAQ dispatch
     */
    public EventBuilderBackEnd(IByteBufferCache eventCache,
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
        this.cacheManager = eventCache;

        // register this object with splicer analysis
        analysis.setDataProcessor(this);

        //get factory object for event payloads
        eventFactory = new EventPayload_v3Factory();
        eventFactory.setByteBufferCache(eventCache);
    }

    /**
     * Increment the count of splicer.execute() calls.
     */
    public void addExecuteCall()
    {
        // XXX do nothing
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
        // XXX do nothing
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
    public void disposeData(ILoadablePayload data)
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
        if (cacheOutputEngine != null) {
            cacheOutputEngine.sendLastAndStop();
        }
        totStopsSent++;
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
     * Get most recent splicer.execute() list length for this run.
     *
     * @return most recent splicer.execute() list length
     */
    public long getCurrentExecuteListLength()
    {
        return execListLen;
    }

    /**
     * Returns the number of units still available in the disk (measured in MB).
     * If it fails to check the disk space, then it returns -1.
     *
     * @return the number of units still available in the disk.
     */
    public int getDiskAvailable()
    {
        return dispatcher.getDiskAvailable();
    }

    /**
     * Returns the total number of units in the disk (measured in MB).
     * If it fails to check the disk space, then it returns -1.
     *
     * @return the total number of units in the disk.
     */
    public int getDiskSize()
    {
        return dispatcher.getDiskSize();
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
     * Compute the subrun number which should follow the specified number.
     *
     * @param num current subrun number
     *
     * @return next subrun number
     */
    private static int getNextSubrunNumber(int num)
    {
        if (num < 0) {
            return -num;
        }

        return -num - 1;
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
     * Get the current subrun number.
     *
     * @return current subrun number
     */
    public int getSubrunNumber()
    {
        return subrunNumber;
    }

    /**
     * Get the total number of events for the specified subrun.
     *
     * @param subrun subrun number
     *
     * @return total number of events sent during the specified subrun
     */
    public long getSubrunTotalEvents(int subrun)
    {
        // return count from current subrun
        if (subrun == subrunNumber) {
            return subrunEventCount;
        }

        SubrunEventCount data =
            subrunEventCountMap.get(new SubrunEventCount(subrun, 0L));
        if (data == null) {
            throw new RuntimeException("Illegal subrun " + subrun);
        }

        return data.getCount();
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
    public ILoadablePayload makeDataPayload(IPayload reqPayload,
                                            List dataList)
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
            LOG.debug("Closing Event " + startTime + " - " + endTime);
        }
        if (LOG.isWarnEnabled() && dataList.size() == 0) {
            LOG.warn("Sending empty event for window [" + startTime + " - " +
                     endTime + "]");
        }

        final int eventType = req.getTriggerType();

        int subnum;
        synchronized (subrunEventCountMap) {
	    if (subrunTransition) { // prepareSubrun has been called
		closeSubRun();
		subrunTransition = false;
	    }
	    if (newSubrunStartTime && startTime.getUTCTimeAsLong() >= subrunStart) {
		// commitSubrun was called & that subrun is here
		closeSubRun();
		subrunNumber = getNextSubrunNumber(subrunNumber);
		newSubrunStartTime = false;
	    }
	    subnum = subrunNumber;
        }
	subrunEventCount++;

	LOG.error("ksb-makeDataPayload(): subrunNumber: " + subrunNumber +
		  ", subrunEventCount: " + subrunEventCount +
		  ", subrunStart: " + subrunStart + ", startTime: " + startTime.getUTCTimeAsLong());
        Payload event =
            eventFactory.createPayload(req.getUID(), ME, startTime, endTime,
                                       eventType, runNumber, subnum, req,
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

            execListLen = 0;

            runNumber = Integer.MIN_VALUE;
            reportedBadRunNumber = false;

            isReset = true;
        }

        super.reset();
    }

    /**
     * Reset the back end during startup.
     */
    public void resetAtStart()
    {
        subrunNumber = 0;
        subrunStart = 0L;
        subrunEventCount = 0L;
        subrunEventCountMap.clear();
	subrunTransition = false;
	newSubrunStartTime = false;

        reset();
    }

    /**
     * Send an output payload.
     *
     * @param output payload being sent
     *
     * @return <tt>true</tt> if event was sent
     */
    public boolean sendOutput(ILoadablePayload output)
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
    private boolean sendToDaqDispatch(ILoadablePayload event)
    {
        EventPayload_v3 tmpEvent = (EventPayload_v3) event;

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
                    LOG.debug("Event " + tmpEvent.getFirstTimeUTC() + "-" +
                              tmpEvent.getLastTimeUTC() +
                              " written to daq-dispatch");
                }

                eventSent = true;
            } catch (DispatchException ex) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Could not dispatch event", ex);
                }
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
    }

    /**
     * Set the start and end times from the current request.
     *
     * @param payload request payload
     */
    public void setRequestTimes(IPayload payload)
    {
        if (LOG.isDebugEnabled()) {
            final ITriggerRequestPayload req = (ITriggerRequestPayload) payload;

            LOG.debug("Filling trigger#" + req.getUID() + " [" +
                      req.getFirstTimeUTC() + "-" + req.getLastTimeUTC() + "]");
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
     * Prepare for a new subrun starting soon.  This will cause
     * subsequent events to be marked with a new subrun number, which
     * will be the negative of the number provided here - until the
     * commitSubrun method is called and the timestamp provided there
     * (identifying the begining of the new subrun) has passed.
     *
     * @param subrunNumber the subrun number which will be starting
     */
    public void prepareSubrun(int subrunNumber)
    {
	LOG.error("ksb-prepareSubrun(" + subrunNumber + ")");

	subrunNumber = -subrunNumber;
	int tmpNum = getNextSubrunNumber(this.subrunNumber);
	if (subrunNumber != tmpNum) {
            LOG.error("prepareSubrun(): Expected subrun number " +
		      this.subrunNumber + " to be followed by " +
		      tmpNum + ", not " + subrunNumber);
	}

	synchronized (subrunEventCountMap) {
	    subrunTransition = true;
	    this.subrunNumber = subrunNumber;
	}
    }

    /**
     * Mark the begining timestamp for when the indicated subrun has
     * started.  This method is used in conjunction with the
     * prepareSubrun() method in that this identifies when events
     * should no longer be marked with the negative of the subrun
     * number.
     *
     * @param subrunNumber the subrun number
     * @param startTime time of first good hit in subrun
     */
    public void commitSubrun(int subrunNumber, long startTime)
    {
	LOG.error("ksb-commitSubrun(" + subrunNumber + ", " + startTime + ")");

	int tmpNum = getNextSubrunNumber(this.subrunNumber);
	if (subrunNumber != tmpNum) {
            LOG.error("commitSubrun(): provided subrun number " +
		      subrunNumber + " does not follow subrun " +
		      this.subrunNumber + ". Subrun number " + tmpNum +
		      " will be used instead.");
	}

	synchronized (subrunEventCountMap) {
	    newSubrunStartTime = true;
	    this.subrunStart = startTime;
	}
    }

    /**
     * Close out the current subrun by sending a subrun databoundary
     * to the dispatcher and storing the count for this ending subrun
     * in the subrunEventCountMap
     */
    private void closeSubRun()
    {
	LOG.error("ksb-closeSubrun(): subrunNumber: " + subrunNumber +
		  ", subrunEventCount: " + subrunEventCount);
        synchronized (subrunEventCountMap) {
	    // ToDo: dataBoundary();
            // save data from previous subrun
            SubrunEventCount newData =
                new SubrunEventCount(this.subrunNumber, subrunEventCount);
            if (subrunEventCountMap.containsKey(newData)) {
                LOG.error("Found multiple counts for subrun number " +
                          this.subrunNumber);
            } else {
                subrunEventCountMap.put(newData, newData);
            }
        }
	subrunEventCount = 0;
    }

    /**
     * Inform back-end processor that the splicer has stopped.
     */
    public void splicerStopped()
    {
        addDataStop();
    }
}
