package engine.entity;

public class User {
    private String username;
    private String passwordHash;
    private boolean isAdmin;

    public User(String username, String passwordHash) {
        setPasswordHash(passwordHash);
        setUsername(username);
        setIsAdmin(false);
    }

    public boolean isAdmin() {
        return isAdmin;
    }

    public void setIsAdmin(boolean isAdmin) {
        this.isAdmin = isAdmin;
    }

    public String getUsername() {
        return username;
    }
    public String getPasswordHash() {
        return passwordHash;
    }
    public void setPasswordHash(String passwordHash) {
        this.passwordHash = passwordHash;
    }
    public void setUsername(String username) {
        this.username = username;
    }
}
