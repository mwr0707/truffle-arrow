package net.wrap_trap.truffle_arrow;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class FilterTest {

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_fields.arrow");
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_fields.arrow").delete();
  }

  @Test
  public void simpleEqualFilterByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select * from ALL_FIELDS where F_INT=2");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("2\t2\ttest2\t2020-05-04 15:48:11.0\t03:20:23\t2020-05-05\t125.456"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleLessThanFilterByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select * from ALL_FIELDS where 2 < F_INT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(7));
      assertThat(results.get(0), is("3\t3\ttest3\t2020-05-04 16:48:11.0\t04:20:23\t2020-05-06\t126.456"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

// TODO Need to implement 'CAST'
//  @Test
//  public void simpleFilterByInt2() throws SQLException {
//    try (
//      Connection conn = DriverManager.getConnection("jdbc:truffle:");
//      PreparedStatement pstmt = conn.prepareStatement(
//        "select * from ALL_FIELDS where '3'=F_INT");
//      ResultSet rs = pstmt.executeQuery()
//    ) {
//      List<String> results = TestUtils.getResults(rs);
//      assertThat(results.size(), is(1));
//      assertThat(results.get(0), is("3\t3\ttest3\t2020-05-04 15:48:11.0"));
//      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
//    }
//  }

  @Test
  public void simpleFilterByLong() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF where N_NATIONKEY=1");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("1\tARGENTINA\t1"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByLong2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF where '1'=N_NATIONKEY");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("1\tARGENTINA\t1"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByString() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF where N_NAME='BRAZIL'");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("2\tBRAZIL\t1"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByString2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF where 'BRAZIL'=N_NAME");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("2\tBRAZIL\t1"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByTimestamp() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_BIGINT, F_VARCHAR, F_TIMESTAMP from ALL_FIELDS where F_TIMESTAMP=timestamp'2020-05-04 17:48:11'");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("4\t4\ttest4\t2020-05-04 17:48:11.0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByTimestamp2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_BIGINT, F_VARCHAR, F_TIMESTAMP from ALL_FIELDS where timestamp'2020-05-04 16:48:11'=F_TIMESTAMP");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("3\t3\ttest3\t2020-05-04 16:48:11.0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByTime() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_TIME from ALL_FIELDS where F_TIME=time'04:20:23'");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("3\t04:20:23"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByTime2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_TIME from ALL_FIELDS where time'05:20:23'=F_TIME");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("4\t05:20:23"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByDate() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_DATE from ALL_FIELDS where F_DATE=date'2020-05-04'");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("1\t2020-05-04"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByDate2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_DATE from ALL_FIELDS where date'2020-05-05'=F_DATE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("2\t2020-05-05"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  // TODO Need to implement 'CAST'
//  @Test
//  public void simpleFilterByFloat() throws SQLException {
//    try (
//      Connection conn = DriverManager.getConnection("jdbc:truffle:");
//      PreparedStatement pstmt = conn.prepareStatement(
//        "select F_INT, F_FLOAT from ALL_FIELDS where F_FLOAT=126.456");
//      ResultSet rs = pstmt.executeQuery()
//    ) {
//      List<String> results = TestUtils.getResults(rs);
//      assertThat(results.size(), is(1));
//      assertThat(results.get(0), is("3\t126.456"));
//      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
//    }
//  }

  @Test
  public void simpleFilterByDouble() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_DOUBLE from ALL_FIELDS where F_DOUBLE=126.456");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("3\t126.456"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }

  @Test
  public void simpleFilterByDouble2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_DOUBLE from ALL_FIELDS where 126.456=F_DOUBLE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("3\t126.456"));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
  }
}
