package edu.upenn.cis.cis455.storage;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class User implements Serializable {

    //////////////////////////////
    //// Work as a database entity,
    //// containing user name & hashed password
    //////////////////////////////
    private final int id;
    private final String username;
    private final String password; // SHA-256 encrypted

    /**
     * Constructor for an User entity.
     * Note, use helper method to hash the provided password
     * before creating a new user.
     * 
     * @param id       : [int], userID
     * @param username : [String], user name
     * @param password : [String], hashed password
     */
    public User(int id, String username, String password) {

        // set id
        this.id = id;

        // set user name
        this.username = username;

        // set the password
        this.password = password;

    }

    public int getID() {

        return this.id;

    }

    public String getUsername() {

        return this.username;

    }

    public String getPassword() {

        return this.password;

    }

    public String toString() {

        return "ID: " + this.id + " , Name: " + this.username;

    }

    /**
     * Helper method to hash the password with SHA-256 algorithm
     * 
     * @param password : [String], unhashed password
     * @return [String], hashed password
     */
    public static String hashPassword(String password) {

        try {

            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            byte[] hashed = digest.digest(password.getBytes(StandardCharsets.UTF_8));

            return new String(hashed, StandardCharsets.UTF_8);

        } catch (NoSuchAlgorithmException nae) {

            nae.printStackTrace();

        } catch (NullPointerException ne) {

            ne.printStackTrace();

        }

        return password;

    }

}
