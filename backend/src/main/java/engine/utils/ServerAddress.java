package engine.utils;

import java.util.ArrayList;
import java.util.List;

public class ServerAddress {
    public static List<String> parse(String addresses) {
        List<String> parsedAddresses = new ArrayList<>();
        for (String address: addresses.split(",")) {
            if (address.trim().startsWith("http")) {
                parsedAddresses.add(address.trim());
            } else {
                parsedAddresses.add("http://" + address.trim());
            }
        }
        return parsedAddresses;
    }
}
