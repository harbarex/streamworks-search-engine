package engine.entity;

public class PopularKeyword {
    private String keyword;
    private int count;

    public PopularKeyword(String keyword, int count) {
        this.keyword = keyword;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
}
