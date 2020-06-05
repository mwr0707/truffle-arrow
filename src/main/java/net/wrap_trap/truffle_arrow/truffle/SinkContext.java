package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.ArrowFieldType;
import org.apache.arrow.vector.FieldVector;

import java.util.List;
import java.util.stream.Collectors;

public class SinkContext {
  private List<FieldVector> vectors;
  private List<ArrowFieldType> arrowFieldTypes;

  public void vectors(List<FieldVector> vectors) {
    this.vectors = vectors;
    this.arrowFieldTypes = vectors.stream().map(f ->
      ArrowFieldType.of(f.getField().getFieldType().getType()))
      .collect(Collectors.toList());
  }

  public ArrowFieldType getArrowFieldType(int index) {
    if (this.arrowFieldTypes == null) {
      throw new IllegalStateException("vectors have not been initialized yet");
    }
    return this.arrowFieldTypes.get(index);
  }
}
