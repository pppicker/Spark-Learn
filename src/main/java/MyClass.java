package streaming;

import java.io.Serializable;

public class MyClass implements Serializable {
    String name = null;
    Integer age = 0;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
