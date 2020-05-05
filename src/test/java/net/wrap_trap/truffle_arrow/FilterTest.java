package net.wrap_trap.truffle_arrow;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FilterTest {
  @BeforeClass
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_fields.arrow");
  }

  @AfterClass
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_fields.arrow").delete();
  }

  @Test
  public void simpleFilterByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select * from ALL_FIELDS where F_INT=2");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("2\t2\ttest2\t2020-05-04 15:48:11.0"));
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
}
