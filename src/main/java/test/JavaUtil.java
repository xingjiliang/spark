package test;

public class JavaUtil {
    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0 || str.trim().equals("-") || str.trim().equals("null")
                || str.trim().equals("") || str.trim().equals("\\N");
    }
}
