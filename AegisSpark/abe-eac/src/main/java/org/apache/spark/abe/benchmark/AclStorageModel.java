/*
 * 集中式 ACL 表空间占用模型（与 company 数据集同规模，多用户列级细粒度授权）。
 */
package org.apache.spark.abe.benchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/** 模拟 Ranger/Hive 风格 ACL grant 行的空间占用。 */
final class AclStorageModel {

  static final String[] COLUMNS = {
      "employee_id", "name", "email", "phone", "salary", "department", "address", "bank_account"
  };
  static final boolean[] SENSITIVE = {
      false, false, true, true, true, false, false, true
  };
  static final String[] DEPTS = {
      "Engineering", "Finance", "HR", "Sales", "Marketing", "Operations", "Legal", "IT"
  };

  /** 单条 ACL grant 行（含 B-tree 索引）估算字节数 */
  static final int ENTRY_BYTES = Integer.parseInt(
      System.getenv().getOrDefault("ACL_ENTRY_BYTES", "248"));

  static final class User {
    final String id;
    final String role;
    final String dept;
    final int clearance;

    User(String id, String role, String dept, int clearance) {
      this.id = id;
      this.role = role;
      this.dept = dept;
      this.clearance = clearance;
    }
  }

  static final class TableAclStats {
    long entries;
    long bytes;
    long plainBytes;
    int rows;
  }

  private final List<User> users;
  private final String hdfsRoot;

  AclStorageModel(int numUsers, String hdfsRoot) {
    this.users = buildUsers(numUsers);
    this.hdfsRoot = hdfsRoot.endsWith("/") ? hdfsRoot : hdfsRoot + "/";
  }

  /** 与 native encryptCompanyTable 一致的行数/明文大小估算 */
  static int recordCount(long companyId) {
    Random rng = new Random(42L + companyId * 7919L);
    // 与 worker_encrypt.cpp: 50 + (rng() % 251) 一致，行数 50~300
    return 50 + rng.nextInt(251);
  }

  static long plainBytes(int nrec) {
    long[] widths = {16, 48, 64, 16, 12, 32, 96, 24};
    long total = 0;
    for (long w : widths) total += w * nrec;
    return total;
  }

  TableAclStats statsForTable(long companyId) {
    int nrec = recordCount(companyId);
    int tableDept = (int) (companyId % DEPTS.length);
    String deptName = DEPTS[tableDept];
    String shard = String.format("part-%04d/", companyId / 10000L);
    String resourceBase = hdfsRoot + shard + String.format("company_%06d.enc", companyId);

    long entries = 0;
    for (int c = 0; c < COLUMNS.length; c++) {
      List<User> grantees = granteesForColumn(COLUMNS[c], SENSITIVE[c], deptName);
      entries += grantees.size();
    }

    TableAclStats st = new TableAclStats();
    st.entries = entries;
    st.bytes = entries * ENTRY_BYTES;
    st.plainBytes = plainBytes(nrec);
    st.rows = nrec;
    return st;
  }

  /**
   * 细粒度列级 ACL，语义对齐 ABE 策略：
   * - 公开列：analyst/auditor 全局可读；同部门 employee/manager/hr 可读
   * - 敏感列：admin+clearance>=5；同部门 manager(clearance>=3)；同部门 hr 可读 email/phone/salary
   */
  List<User> granteesForColumn(String column, boolean sensitive, String tableDept) {
    List<User> out = new ArrayList<>();
    for (User u : users) {
      if (!sensitive) {
        if ("analyst".equals(u.role) || "auditor".equals(u.role)) {
          out.add(u);
        } else if (tableDept.equals(u.dept)
            && ("employee".equals(u.role) || "manager".equals(u.role) || "hr".equals(u.role))) {
          out.add(u);
        }
      } else {
        if ("admin".equals(u.role) && u.clearance >= 5) {
          out.add(u);
        } else if ("manager".equals(u.role) && tableDept.equals(u.dept) && u.clearance >= 3) {
          out.add(u);
        } else if ("hr".equals(u.role) && tableDept.equals(u.dept)
            && ("email".equals(column) || "phone".equals(column) || "salary".equals(column))) {
          out.add(u);
        }
      }
    }
    return out;
  }

  static List<User> buildUsers(int numUsers) {
    List<User> out = new ArrayList<>(numUsers);
    Random rng = new Random(20240902L);
    for (int i = 0; i < numUsers; i++) {
      double r = rng.nextDouble();
      String role;
      int clearance;
      if (r < 0.40) {
        role = "analyst";
        clearance = 1 + rng.nextInt(3);
      } else if (r < 0.45) {
        role = "admin";
        clearance = 4 + rng.nextInt(2);
      } else if (r < 0.55) {
        role = "manager";
        clearance = 2 + rng.nextInt(4);
      } else if (r < 0.65) {
        role = "hr";
        clearance = 2 + rng.nextInt(3);
      } else if (r < 0.70) {
        role = "auditor";
        clearance = 3 + rng.nextInt(2);
      } else {
        role = "employee";
        clearance = 1 + rng.nextInt(2);
      }
      String dept = DEPTS[i % DEPTS.length];
      out.add(new User(String.format("user-%06d", i), role, dept, clearance));
    }
    return Collections.unmodifiableList(out);
  }

  int userCount() {
    return users.size();
  }
}
