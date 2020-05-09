package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.sql.SqlKind;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

@NodeInfo(shortName = "=")
abstract class ExprFilter extends ExprBinary {

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, IntVector left, Long right) {
    return eval(sqlKind, left, right.intValue(), false);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, Long left, IntVector right) {
    return eval(sqlKind, right, left.intValue(), true);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, BigIntVector left, Integer right) {
    return eval(sqlKind, left, right.longValue(), false);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, Integer left, BigIntVector right) {
    return eval(sqlKind, right, left.longValue(), true);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, TimeStampSecTZVector left, Instant right) {
    return eval(sqlKind, left, right.toEpochMilli(), false);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, Instant left, TimeStampSecTZVector right) {
    return eval(sqlKind, right, left.toEpochMilli(), true);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, TimeSecVector left, LocalTime right) {
    return eval(sqlKind, left, right.toSecondOfDay(), false);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, LocalTime left, TimeSecVector right) {
    return eval(sqlKind, right, left.toSecondOfDay(), true);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, DateDayVector left, LocalDate right) {
    return eval(sqlKind, left, Long.valueOf(right.toEpochDay()).intValue(), false);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, LocalDate left, DateDayVector right) {
    return eval(sqlKind, right, Long.valueOf(left.toEpochDay()).intValue(), true);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, FieldVector left, Object right) {
    return eval(sqlKind, left, right, false);
  }

  @Specialization
  protected UInt4Vector filter(SqlKind sqlKind, Object left, FieldVector right) {
    return eval(sqlKind, right, left, true);
  }

  protected UInt4Vector eval(SqlKind sqlKind, FieldVector left, Object right, boolean reverse) {
    UInt4Vector selectionVector = ArrowUtils.createSelectionVector();
    int selectionIndex = 0;

    selectionVector.setValueCount(left.getValueCount());
    for (int i = 0; i < left.getValueCount(); i++) {
      Object o = left.getObject(i);
      if (o instanceof Text) {
        o = ((Text) o).toString();
      }
      if (compare(sqlKind, (Comparable) o, right, reverse)) {
        selectionVector.set(selectionIndex ++, i);
      }
    }
    selectionVector.setValueCount(selectionIndex);
    return selectionVector;
  }

  protected boolean compare(SqlKind sqlKind, Comparable left, Object right, boolean reverse) {
    switch(sqlKind) {
      case LESS_THAN:
        if (reverse) {
          return left.compareTo(right) > 0;
        }
        return left.compareTo(right) < 0;
      case GREATER_THAN:
        if (reverse) {
          return left.compareTo(right) < 0;
        }
        return left.compareTo(right) > 0;
      case LESS_THAN_OR_EQUAL:
        if (reverse) {
          return left.compareTo(right) >= 0;
        }
        return left.compareTo(right) <= 0;
      case GREATER_THAN_OR_EQUAL:
        if (reverse) {
          return left.compareTo(right) <= 0;
        }
        return left.compareTo(right) >= 0;
      case EQUALS:
        return left.compareTo(right) == 0;
      case NOT_EQUALS:
        return left.compareTo(right) != 0;
      default:
        throw new UnsupportedOperationException("sqlKind: " + sqlKind);
    }
  }
}
