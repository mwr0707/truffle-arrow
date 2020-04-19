package net.wrap_trap.truffle_arrow;

import com.oracle.truffle.api.frame.VirtualFrame;
import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

/**
 * Filter for Apache Arrow
 */
public class ArrowFilter extends SingleRel implements ArrowRel {

  private RexProgram program;
  private RexNode condition;

  public ArrowFilter(RelOptCluster cluster,
                     RelTraitSet traitSet,
                     RelNode input,
                     RexProgram program,
                     RexNode condition) {
    super(cluster, traitSet, input);
    this.program = program;
    this.condition = condition;
    assert !program.containsAggs();
  }

  @Override
  public ArrowFilter copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 1;
    return create(traitSet, inputs.get(0), this.program, this.condition);
  }

  public static ArrowFilter create(final RelTraitSet traitSet, final RelNode input,
                                   final RexProgram program, final RexNode condition) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    return new ArrowFilter(cluster, traitSet, input, program, condition);
  }

  public RowSource compile(ThenRowSink next) {
    ThenRowSink wrapped =
      sourceFrame ->  FilterSink.createSink(sourceFrame, this.condition, next);

    if (this.input != null) {
      ArrowRel arrowRel = (ArrowRel) this.input;
      return arrowRel.compile(wrapped);
    } else {
      return TerminalSink.compile(wrapped);
    }

    // where 句の parse
    // RexCall から node を作る
    // node.compile を呼ぶ
  }
}