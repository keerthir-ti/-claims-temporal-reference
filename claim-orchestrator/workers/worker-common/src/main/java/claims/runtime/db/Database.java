package claims.runtime.db;

import claims.runtime.Env;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;

/** Connection pool for a database given by {PREFIX}_JDBC_URL, {PREFIX}_DB_USER, {PREFIX}_DB_PASSWORD. */
public final class Database {
  private Database() {}

  public static DataSource pool(String prefix, int size) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(Env.get(prefix + "_JDBC_URL", "jdbc:postgresql://localhost:5432/claims"));
    config.setUsername(Env.get(prefix + "_DB_USER", "claims"));
    config.setPassword(Env.get(prefix + "_DB_PASSWORD", "claims"));
    config.setMaximumPoolSize(size);
    config.setPoolName(prefix.toLowerCase());
    return new HikariDataSource(config);
  }
}
