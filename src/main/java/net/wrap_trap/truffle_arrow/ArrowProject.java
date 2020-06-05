package net.wrap_trap.truffle_arrow;


import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class ArrowProject  extends Project implements ArrowRel {

  private List<? extends RexNode> projects;

  public ArrowProject(RelOptCluster cluster,
                      RelTraitSet traitSet,
                      RelNode input,
                      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
    this.projects = projects;
  }

  @Override
  public ArrowProject copy(RelTraitSet traitSet, RelNode input,
                           List<RexNode> projects, RelDataType rowType) {
    return new ArrowProject(getCluster(), traitSet, input, projects, rowType);
  }

  public ThenRowSink createRowSink(ThenRowSink next) {
    return
      sourceFrame -> RelProject.compile(sourceFrame, projects, next);
  }
}
