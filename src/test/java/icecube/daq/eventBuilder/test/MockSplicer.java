package icecube.daq.eventBuilder.test;

import icecube.daq.splicer.OrderingException;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SplicedAnalysis;
import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.SplicerListener;
import icecube.daq.splicer.StrandTail;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.util.List;

public class MockSplicer
    implements Splicer<Spliceable>
{
    public MockSplicer()
    {
    }

    public void addSpliceableChannel(SelectableChannel x0)
        throws IOException
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void addSplicerListener(SplicerListener x0)
    {
        throw new Error("Unimplemented");
    }

    public void analyze()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public StrandTail beginStrand()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void dispose()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void forceStop()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public SplicedAnalysis getAnalysis()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public State getState()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public int getStrandCount()
    {
        throw new Error("Unimplemented");
    }

    public List pendingChannels()
    {
        throw new Error("Unimplemented");
    }

    public List pendingStrands()
    {
        throw new Error("Unimplemented");
    }

    public void removeSpliceableChannel(SelectableChannel x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void removeSplicerListener(SplicerListener x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void start()
    {
        throw new Error("Unimplemented");
    }

    public void start(Spliceable x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void stop()
    {
        throw new Error("Unimplemented");
    }

    public void stop(Spliceable x0)
        throws OrderingException
    {
        throw new Error("Unimplemented");
    }
}
