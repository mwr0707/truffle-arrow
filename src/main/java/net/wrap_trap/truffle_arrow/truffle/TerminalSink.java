package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class TerminalSink extends RowSource {

  private RowSink then;
  private FrameDescriptor frameDescriptor;
  private FrameDescriptorPart sourceFrame;

  public static RowSource compile(ThenRowSink next) {
    FrameDescriptorPart sourceFrame = FrameDescriptorPart.root(0);
    return new TerminalSink(sourceFrame, next.apply(sourceFrame));
  }

  private TerminalSink(FrameDescriptorPart sourceFrame, RowSink then) {
    this.frameDescriptor = sourceFrame.frame();
    this.sourceFrame = sourceFrame;
    this.then = then;
  }

  @Override
  protected void executeVoid() throws UnexpectedResultException {
    then.executeVoid(
      Truffle.getRuntime().createVirtualFrame(new Object[] { }, frameDescriptor),
      this.sourceFrame, new SinkContext());
  }
}
