import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaTest {
    public static void main(String[] args) {
        String[] stringArray = new String[]{"a", "b", "c"};
        List<String> stringList = new ArrayList(Arrays.asList(stringArray));
        List<String> tempList = new ArrayList();
        for (String p : stringList) {
            if (!"gender".equals(p) && !"age".equals(p)) {
                tempList.add(p.concat("_c"));
                tempList.add(p.concat("_d"));
            }
        }
        stringList.addAll(tempList);
        stringList.forEach(string -> {
            System.out.println(string);
        });
    }
}
