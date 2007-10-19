package icecube.daq.eventBuilder;

import icecube.daq.common.DAQCmdInterface;

import icecube.daq.io.Dispatcher;
import icecube.daq.io.FileDispatcher;
import icecube.daq.io.SpliceablePayloadReader;

import icecube.daq.eventBuilder.backend.EventBuilderBackEnd;

import icecube.daq.eventBuilder.monitoring.MonitoringData;

import icecube.daq.juggler.component.DAQCompServer;
import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.juggler.component.DAQComponent;
import icecube.daq.juggler.component.DAQConnector;

import icecube.daq.juggler.mbean.MemoryStatistics;
import icecube.daq.juggler.mbean.SystemStatistics;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.MasterPayloadFactory;
import icecube.daq.payload.VitreousBufferCache;

import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.SplicerImpl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Event builder component.
 */
public class EBComponent
    extends DAQComponent
{
    /** Component name. */
    private static final String COMPONENT_NAME =
        DAQCmdInterface.DAQ_EVENTBUILDER;

    /** Message logger. */
    private static final Log LOG = LogFactory.getLog(EBComponent.class);

    private GlobalTriggerReader gtInputProcess;
    private SpliceablePayloadReader rdoutDataInputProcess;

    private EventBuilderSPreqPayloadOutputEngine spReqOutputProcess;
    private EventBuilderSPcachePayloadOutputEngine spFlushOutputProcess;

    private EventBuilderBackEnd backEnd;
    private SPDataAnalysis splicedAnalysis;

    private Dispatcher dispatcher;

    /**
     * Create an event builder component.
     */
    public EBComponent()
    {
        super(COMPONENT_NAME, 0);

        final int compId = 0;

        IByteBufferCache rdoutDataMgr = new VitreousBufferCache();
        addCache(DAQConnector.TYPE_READOUT_DATA, rdoutDataMgr);
        MasterPayloadFactory rdoutDataFactory =
            new MasterPayloadFactory(rdoutDataMgr);

        IByteBufferCache trigBufMgr = new VitreousBufferCache();
        addCache(DAQConnector.TYPE_GLOBAL_TRIGGER, trigBufMgr);
        MasterPayloadFactory trigFactory =
            new MasterPayloadFactory(trigBufMgr);

        IByteBufferCache genMgr = new VitreousBufferCache();
        addCache(genMgr);

        addMBean("jvm", new MemoryStatistics());
        addMBean("system", new SystemStatistics());

        MonitoringData monData = new MonitoringData();
        addMBean("backEnd", monData);

        splicedAnalysis = new SPDataAnalysis(rdoutDataFactory);
        Splicer splicer = new SplicerImpl(splicedAnalysis);
        splicer.addSplicerListener(splicedAnalysis);
        addSplicer(splicer);

        dispatcher = new FileDispatcher("physics");

        backEnd =
            new EventBuilderBackEnd(genMgr, splicer, splicedAnalysis,
                                    dispatcher);

        try {
            gtInputProcess =
                new GlobalTriggerReader(COMPONENT_NAME, backEnd, trigFactory,
                                        trigBufMgr);
        } catch (IOException ioe) {
            throw new Error("Couldn't create GlobalTriggerReader", ioe);
        }
        addMonitoredEngine(DAQConnector.TYPE_GLOBAL_TRIGGER, gtInputProcess);

        spReqOutputProcess =
            new EventBuilderSPreqPayloadOutputEngine(COMPONENT_NAME, compId,
                                                     "spReqOutput");
        addMonitoredEngine(DAQConnector.TYPE_READOUT_REQUEST,
                           spReqOutputProcess, true);

        try {
            rdoutDataInputProcess =
                new SpliceablePayloadReader(COMPONENT_NAME, splicer,
                                            rdoutDataFactory);
        } catch (IOException ioe) {
            throw new Error("Couldn't create ReadoutDataReader", ioe);
        }
        addMonitoredEngine(DAQConnector.TYPE_READOUT_DATA,
                           rdoutDataInputProcess);

        final boolean skipFlush = true;

        if (!skipFlush) {
            // TODO: don't add this output engine; it should go away
            spFlushOutputProcess =
                new EventBuilderSPcachePayloadOutputEngine(COMPONENT_NAME,
                                                           compId,
                                                           "spFlushOutput");
        }

        // connect pieces together
        gtInputProcess.registerStringProcReqOutputEngine(spReqOutputProcess);
        if (!skipFlush) {
            backEnd.registerStringProcCacheOutputEngine(spFlushOutputProcess);
        }
        spReqOutputProcess.registerBufferManager(genMgr);
        if (!skipFlush) {
            spFlushOutputProcess.registerBufferManager(genMgr);
        }

        monData.setGlobalTriggerInputMonitor(gtInputProcess);
        monData.setBackEndMonitor(backEnd);
    }

    /**
     * Begin packaging events for the specified subrun.
     *
     * @param subrunNumber subrun number
     * @param startTime time of first good hit in subrun
     */
    public void commitSubrun(int subrunNumber, long startTime)
    {
        if (subrunNumber == 0) {
            throw new RuntimeException("Subrun number cannot be zero");
        }

        if (subrunNumber < 0) {
            LOG.error("Committed subrun number " + subrunNumber +
                      " should be not negative");
            subrunNumber = -subrunNumber;
        }

        backEnd.setSubrunNumber(subrunNumber, startTime);
    }

    /**
     * Get the number of events for the given subrun.
     * NOTE: This should only be implemented by the event builder component.
     *
     * @param subrun subrun number
     *
     * @return number of events for the subrun
     *
     * @throws DAQCompException if the subrun number is not valid
     */
    public long getEvents(int subrun)
        throws DAQCompException
    {
        try {
            return backEnd.getSubrunTotalEvents(subrun);
        } catch (RuntimeException rte) {
            throw new DAQCompException(rte.getMessage());
        }
    }

    /**
     * Prepare for the subrun by marking events untrustworthy.
     *
     * @param subrunNumber subrun number
     */
    public void prepareSubrun(int subrunNumber)
    {
        if (subrunNumber == 0) {
            throw new RuntimeException("Subrun number cannot be zero");
        }

        if (subrunNumber < 0) {
            LOG.error("Preparatory subrun number " + subrunNumber +
                      " should be not negative");
            subrunNumber = -subrunNumber;
        }

        backEnd.setSubrunNumber(-subrunNumber, Long.MIN_VALUE);
    }

    /**
     * Set the destination directory where the dispatch files will be saved.
     *
     * @param dirName The absolute path of directory where the dispatch files will be stored.
     */
    public void setDispatchDestStorage(String dirName) {
        dispatcher.setDispatchDestStorage(dirName);
    }

    /**
     * Set the maximum size of the dispatch file.
     *
     * @param maxFileSize the maximum size of the dispatch file.
     */
    public void setMaxFileSize(long maxFileSize) {
        dispatcher.setMaxFileSize(maxFileSize);
    }

    /**
     * Set the run number inside this component.
     *
     * @param runNumber run number
     */
    public void setRunNumber(int runNumber)
    {
        backEnd.reset();
        backEnd.setRunNumber(runNumber);
        splicedAnalysis.setRunNumber(runNumber);
    }

    /**
     * Return this component's svn version id as a String.
     *
     * @return svn version id as a String
     */
    public String getVersionInfo()
    {
	return "$Id: EBComponent.java 2168 2007-10-20 01:15:02Z ksb $";
    }


    /**
     * Run a DAQ component server.
     *
     * @param args command-line arguments
     *
     * @throws DAQCompException if there is a problem
     */
    public static void main(String[] args)
        throws DAQCompException
    {
        new DAQCompServer(new EBComponent(), args);
    }
}
