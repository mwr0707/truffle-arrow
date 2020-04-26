package net.wrap_trap.truffle_arrow;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Query tests for ArrowTable
 */
public class JdbcTest {

  @BeforeClass
  public static void setupOnce() throws ClassNotFoundException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
  }

  @Test
  public void simpleProjection() throws SQLException {
    try (
          Connection conn = DriverManager.getConnection("jdbc:truffle:");
          PreparedStatement pstmt = conn.prepareStatement("select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF");
          ResultSet rs = pstmt.executeQuery()
        ) {
          ResultSetMetaData meta = rs.getMetaData();
          int count = meta.getColumnCount();
          assertThat(count, is(3));
          assertThat(meta.getColumnName(1), is("N_NATIONKEY"));
          assertThat(meta.getColumnName(2), is("N_NAME"));
          assertThat(meta.getColumnName(3), is("N_REGIONKEY"));

        List<String> results =
          new ResultSetSpliterator<>(rs, r -> Arrays.asList(
            r.getObject(1),
            r.getObject(2),
            r.getObject(3)).stream().map(o -> o == null ? "" : o.toString()).collect(Collectors.joining("\t")))
            .stream().collect(Collectors.toList());

        System.out.println(results);

        assertThat(results.size(), is(25));
        assertThat(results.get(0), is("0\tALGERIA\t0"));
        assertThat(results.get(24), is("24\tUNITED STATES\t1"));
    }
  }

  @Test
  public void simpleFilter() throws SQLException {
    try (
          Connection conn = DriverManager.getConnection("jdbc:truffle:");
          PreparedStatement pstmt = conn.prepareStatement(
            "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF where N_NATIONKEY='1'");
          ResultSet rs = pstmt.executeQuery()
    ) {
      ResultSetMetaData meta = rs.getMetaData();
      int count = meta.getColumnCount();
      assertThat(count, is(3));
      assertThat(meta.getColumnName(1), is("N_NATIONKEY"));
      assertThat(meta.getColumnName(2), is("N_NAME"));
      assertThat(meta.getColumnName(3), is("N_REGIONKEY"));

      List<String> results =
        new ResultSetSpliterator<>(rs, r -> Arrays.asList(
          r.getObject(1),
          r.getObject(2),
          r.getObject(3)).stream().map(o -> o == null ? "" : o.toString()).collect(Collectors.joining("\t")))
          .stream().collect(Collectors.toList());

      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("1\tARGENTINA\t1"));
    }


    // a = 1
    // 1 = a
    // str = 1 // validate NG
    // 1 = str // validate NG
    // num = "1" // validate NG
    // "1" = num // validate NG
  }

  @Test
  public void filterIsNull() {
    // a is null
    // a is not null
  }

  @Test
  public void filterAndOr() {
    // a = 1 and b = 2
    // a = 1 or b = 2
    // a = 1 and (b = 2 and c = 3)
    // a = 1 or (b = 2 or (c = 3 or d = 4))
    // (a = 1 and b = 2) or (c = 3 and (d = 4 or e = 5))
    // ((a = 1 or b = 2) and c = 3) or (d = 4 or (e = 5 and f = 6))
  }
}
