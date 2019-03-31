import java.util.Stack;

public class Test {
  public static void main(String[] args){
    Stack<Character> stack = new Stack<Character>();
    String str = "abcd";
    char[] chars = str.toCharArray();
    for (char aChar : chars) {
      stack.push(aChar);
    }
    StringBuffer strBu = new StringBuffer();
    for (char aChar : chars) {
      strBu.append(stack.pop());
    }
    System.out.println(strBu.toString());
  }
}
