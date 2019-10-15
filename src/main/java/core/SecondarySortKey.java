package core;

import scala.math.Ordered;

import java.io.Serializable;

public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {
    private static final long serialVersionUID = -4055147656585146430L;

    //在自定义Key中定义需要排序的字段
    //为要进行排序的字段提供getter,setter,hashcode和equals方法
    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean $less(SecondarySortKey other) {
        if(this.first < other.getFirst()) {
            return true;
        } else if(this.first == other.getFirst() && this.second < other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey other) {
        if(this.first > other.getFirst()){
            return true;
        } else if(this.first == other.getFirst() && this.second > other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey other) {
        if(this.$less(other)) {
            return true;
        } else if(this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey other) {
        if(this.$greater(other)){
            return true;
        } else if(this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(SecondarySortKey other) {
        if(this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }

    @Override
    public int compareTo(SecondarySortKey other) {
        if(this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }
}
