package crawler;

/**
 * Adapted by Daniel Stekol from CIS 555 homework material (modified to separate queries and anchors)
 * Helper class that shows how to parse URLs to obtain host name, port number
 * and file path
 */
public class URLInfo {
    private String hostName;
    private int portNo;
    private String filePath;
    private String anchor;

    private String query;

    private boolean isSecure = false;

    /**
     * Constructor called with raw URL as input.
     */
    public URLInfo(String docURL) {
        if (docURL == null || docURL.equals(""))
            return;
        docURL = docURL.trim();

        if (docURL.startsWith("https://")) {
            isSecure = true;
            docURL = docURL.replaceFirst("https:", "http:");
        }

        if (!docURL.startsWith("http://") || docURL.length() < 8)
            return;
        // Stripping off 'http://'
        docURL = docURL.substring(7);
        
        int anchorIndex = docURL.indexOf("#");
        if (anchorIndex < 0) {
            anchor = "";
        }
        else {
            anchor = docURL.substring(anchorIndex);
            docURL = docURL.substring(0, anchorIndex);
        }
        
        int queryIndex = docURL.indexOf("?");
        if (queryIndex < 0) {
            query = "";
        }
        else {
            query = docURL.substring(queryIndex);
            docURL = docURL.substring(0, queryIndex);
        }

        int i = 0;
        while (i < docURL.length()) {
            char c = docURL.charAt(i);
            if (c == '/')
                break;
            i++;
        }
        String address = docURL.substring(0, i);
        if (i == docURL.length())
            filePath = "";
        else
            filePath = docURL.substring(i); // starts with '/'
        if (address.equals("/") || address.equals(""))
            return;
        if (address.indexOf(':') != -1) {
            String[] comp = address.split(":", 2);
            hostName = comp[0].trim();
            try {
                portNo = Integer.parseInt(comp[1].trim());
            } catch (NumberFormatException nfe) {
                portNo = isSecure ? 443 : 80;
            }
        } else {
            hostName = address;
            portNo = isSecure ? 443 : 80;
        }
    }

    public URLInfo(String hostName, String filePath) {
        this.hostName = hostName;
        this.filePath = filePath;
        this.portNo = 80;
        query = "";
        anchor = "";
    }

    public URLInfo(String hostName, int portNo, String filePath) {
        this.hostName = hostName;
        this.portNo = portNo;
        this.filePath = filePath;
        query = "";
        anchor = "";
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String s) {
        hostName = s;
    }

    public int getPortNo() {
        return portNo;
    }

    public void setPortNo(int p) {
        portNo = p;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String fp) {
        filePath = fp;
    }

    public boolean isSecure() {
        return isSecure;
    }

    public void setSecure(boolean sec) {
        isSecure = sec;
    }

    public String toString() {
        return (isSecure ? "https://" : "http://") + hostName + ":" + portNo + filePath + query;
    }
    
    public String toStringWithAnchor() {
        return toString() + anchor;
    }
    
    public String getAnchor() {
        return anchor;
    }

    public String getQuery() {
        return query;
    }
}
