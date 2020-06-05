package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowFieldType;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexNode;

import java.util.List;


public class FilterSink extends RowSink {

  public static FilterSink createSink(FrameDescriptorPart sourceFrame, RexNode condition, ThenRowSink next) {
    RowSink rowSink = next.apply(sourceFrame);
    return new FilterSink(CompileExpr.compile(sourceFrame, condition), rowSink);
  }

  RowSink then;
  ExprBase conditionExpr;

  private FilterSink(ExprBase conditionExpr, RowSink then) {
    this.conditionExpr = conditionExpr;
    this.then = then;
  }

  @Override
  public void executeVoid(
    VirtualFrame frame, FrameDescriptorPart sourceFrame, SinkContext context) throws UnexpectedResultException {
    UInt4Vector selectionVector = (UInt4Vector) conditionExpr.executeGeneric(frame);
    FrameSlot slot1 = sourceFrame.findFrameSlot(1);
    if (slot1 == null) {
      // TODO
      // slot1 = this.sourceFrame.addFrameSlot(1);
      throw new IllegalStateException("slot1 == null");
    }
    frame.setObject(slot1, selectionVector);
    then.executeVoid(frame, sourceFrame, context);
  }
}
