package proyectotbd2;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import java.util.ArrayList;

public class DbConnector {

    private Cluster cluster;
    private Session session;

    public void Connect(String seeds, int port) {
        cluster = Cluster.builder().addContactPoint(seeds).withPort(port).build();
        final Metadata metadata = cluster.getMetadata();
        for (final Host host : metadata.getAllHosts()) {
            System.out.println("driver version " + host.getCassandraVersion());
        }
        session = cluster.connect();
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        cluster.close();
    }

    public ResultSet Select(String table) {
        ResultSet rs = null;
        String selectQuery = "SELECT * FROM autoescuela_keyspace." + table;
        PreparedStatement ps = session.prepare(selectQuery);
        BoundStatement bs = ps.bind();
        rs = session.execute(bs);
        return rs;
    }

    public void Delete(String table, Object id) {
        ResultSet rs = null;
        rs = Select(table);
        if (id instanceof String) {
            id = "'" + id + "'";
        }
        String idName = rs.getColumnDefinitions().getName(0);
        String selectQuery = "DELETE FROM autoescuela_keyspace." + table + " WHERE " + idName + "=" + id;
        PreparedStatement ps = session.prepare(selectQuery);
        BoundStatement bs = ps.bind();
        session.execute(bs);
    }

    public void Update(String table, Object id, ArrayList<Object> values) {
        ResultSet rs = null;
        rs = Select(table);
        String stringCulera = "";
        for (int i = 0; i < values.size(); i++) {
            Object value = "";
            if (values.get(i) instanceof String) {
                value = "'" + values.get(i) + "'";
            } else {
                value = values.get(i);
            }
            stringCulera += rs.getColumnDefinitions().getName(i + 1) + "=" + value;
            if (i != values.size() - 1) {
                stringCulera += ", ";
            }
        }
        if (id instanceof String) {
            id = "'" + id + "'";
        }
        String idName = rs.getColumnDefinitions().getName(0);
        String selectQuery = "UPDATE autoescuela_keyspace." + table + " SET " + stringCulera + " WHERE " + idName + "=" + id;
        System.out.println(selectQuery);
        PreparedStatement ps = session.prepare(selectQuery);
        BoundStatement bs = ps.bind();
        session.execute(bs);
    }

    public void Insert(ArrayList<Object> values, String table) {
        ResultSet rs = null;
        rs = Select(table);
        String attributes = "(";
        for (int i = 0; i < rs.getColumnDefinitions().size(); i++) {
            attributes += rs.getColumnDefinitions().getName(i);
            if (i != rs.getColumnDefinitions().size() - 1) {
                attributes += ",";
            }
        }
        attributes += ")";
        String insertQuery = "INSERT INTO autoescuela_keyspace." + table + " " + attributes
                + "VALUES ";
        String add = "(";
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) instanceof String) {
                add += "'" + values.get(i) + "'";
            } else {
                add += values.get(i);
            }
            if (i != values.size() - 1) {
                add += ",";
            }
        }
        add += ")";
        insertQuery += add;
        PreparedStatement ps = session.prepare(insertQuery);
        BoundStatement bs = ps.bind();
        session.execute(bs);
    }
}
