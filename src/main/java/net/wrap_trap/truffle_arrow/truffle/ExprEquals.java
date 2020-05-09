package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.apache.calcite.sql.SqlKind;

import java.util.Objects;

@NodeInfo(shortName = "=")
abstract class ExprEquals extends ExprBinary {


  @Specialization
  protected boolean eq(SqlKind sqlKind, boolean left, boolean right) {
    return left == right;
  }

  @Specialization
  protected boolean eq(SqlKind sqlKind, long left, long right) {
    return left == right;
  }

  @Specialization
  protected boolean eq(SqlKind sqlKind, double left, double right) {
    return left == right;
  }

  @Specialization
  protected SqlNull eq(SqlKind sqlKind, SqlNull left, Object right) {
    return SqlNull.INSTANCE;
  }

  @Specialization
  protected SqlNull eq(SqlKind sqlKind, Object left, SqlNull right) {
    return SqlNull.INSTANCE;
  }

  @Specialization
  protected boolean eq(SqlKind sqlKind, String left, String right) {
    return Objects.equals(left, right);
  }

  @Specialization
  @CompilerDirectives.TruffleBoundary
  protected boolean eq(SqlKind sqlKind, Object left, Object right) {
    return left == right;
  }
}
