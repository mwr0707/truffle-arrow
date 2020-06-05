package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowFieldType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * The expression part of a SELECT query
 */
public class RelProject extends RowTransform {
  @Children
  private final StatementWriteLocal[] select;

  public static RelProject compile(FrameDescriptorPart sourceFrame, List<? extends RexNode> project, ThenRowSink next) {
    StatementWriteLocal[] select = new StatementWriteLocal[project.size()];
    FrameDescriptorPart frame = sourceFrame.push(select.length);

    for (int i = 0; i < project.size(); i++) {
      RexNode child = project.get(i);
      FrameSlot slot = frame.findFrameSlot(i);
      ExprBase compiled = CompileExpr.compile(sourceFrame, child);

      select[i] = StatementWriteLocalNodeGen.create(compiled, slot);
    }

    return new RelProject(select, next.apply(frame));
  }

  public RelProject(StatementWriteLocal[] select, RowSink then) {
    super(then);

    this.select = select;
  }

  @Override
  @ExplodeLoop
  public void executeVoid(VirtualFrame frame, FrameDescriptorPart sourceFrame, SinkContext context) throws UnexpectedResultException {
    for (StatementWriteLocal each : select)
      each.executeVoid(frame);

    then.executeVoid(frame, sourceFrame, context);
  }
}