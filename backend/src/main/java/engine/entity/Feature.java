package engine.entity;

public class Feature {
    private String name;
    private float coeff;
    private boolean useLog;

    public Feature() {
        this.name = "";
        this.coeff = 0;
        this.useLog = false;
    }

    public Feature(String name, float coeff, boolean useLog) {
        this.name = name;
        this.coeff = coeff;
        this.useLog = useLog;
    }

    public boolean isUseLog() {
        return useLog;
    }

    public void setUseLog(boolean useLog) {
        this.useLog = useLog;
    }

    public float getCoeff() {
        return coeff;
    }

    public void setCoeff(float coeff) {
        this.coeff = coeff;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        return name + " " + coeff + " " + useLog;
    }
}
