package storage;

public class MySQLConfig {
    private String dbname;
    private String username;
    private String password;
    private String hostname;
    private String port;

    public MySQLConfig(String dbname, String username, String password, String hostname, String port) {
        this.setDbname(dbname);
        this.setUsername(username);
        this.setPassword(password);
        this.setHostname(hostname);
        this.setPort(port);
    }

    public String getPort() {
        return port;
    }
    public String getDbname() {
        return dbname;
    }
    public void setDbname(String dbname) {
        this.dbname = dbname;
    }
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public String getHostname() {
        return hostname;
    }
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
    public void setPort(String port) {
        this.port = port;
    }
}
