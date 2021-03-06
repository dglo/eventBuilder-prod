package icecube.daq.eventBuilder;

import icecube.daq.eventBuilder.backend.EventBuilderBackEnd;
import icecube.daq.io.PushStreamReader;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.impl.TriggerRequestFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

public class GlobalTriggerReader
    extends PushStreamReader
{
    private static final Logger LOG =
        Logger.getLogger(GlobalTriggerReader.class);

    /** back-end processor which digests trigger requests. */
    private EventBuilderBackEnd backEnd;

    /** main buffer cache. */
    /**
     * The Engine that does the actual demultiplexing
     * of the trigger requests to the SPs.
     */
    private EventBuilderTriggerRequestDemultiplexer demuxer;

    /** standalone trigger/readout request factory. */
    private TriggerRequestFactory trFactory;

    /**
     * Create an instance of this class.
     */
    public GlobalTriggerReader(String name, EventBuilderBackEnd backEnd,
                               TriggerRequestFactory trigReqFactory,
                               IByteBufferCache bufMgr)
        throws IOException
    {
        super(name);

        if (backEnd == null) {
            throw new IllegalArgumentException("Back-end processor cannot" +
                                               " be null");
        }
        this.backEnd = backEnd;

        if (trigReqFactory == null) {
            throw new IllegalArgumentException("Trigger request factory" +
                                               " cannot be null");
        }

        this.trFactory = trigReqFactory;
    }

    public long getReceivedMessages()
    {
        return getDequeuedMessages();
    }

    @Override
    public void pushBuffer(ByteBuffer buf)
        throws IOException
    {
        ITriggerRequestPayload pay;
        try {
            pay = trFactory.createPayload(buf, 0);
        } catch (Exception ex) {
            LOG.error("Cannot create trigger request", ex);
            throw new IOException("Cannot create trigger request");
        }

        try {
            ((IPayload) pay).loadPayload();
        } catch (Exception ex) {
            LOG.error("Cannot load trigger request", ex);
            throw new IOException("Cannot load trigger request");
        }

        // send readout requests
        demuxer.demux(pay);

        // add trigger request to back-end queue
        backEnd.addRequest(pay);
    }

    public void registerDemultiplexer
        (EventBuilderTriggerRequestDemultiplexer demuxer)
    {
        this.demuxer = demuxer;
    }

    @Override
    public void sendStop()
    {
        try {
            backEnd.addRequestStop();
        } catch (IOException ioe) {
            LOG.error("Cannot add stop to backend request queue");
        }

        demuxer.sendStopMessage();
    }

    @Override
    public void startProcessing()
    {
        super.startProcessing();
        backEnd.resetAtStart();
    }
}
