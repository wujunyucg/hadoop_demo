package demo6;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

public class NationUDF extends UDF {

    public static Map<String, String> nationMap = new HashMap<String, String>();
    static {
        nationMap.put("China", "zhonguo");
        nationMap.put("Japan", "riben");
        nationMap.put("USA", "meiguo");
    }
    Text text = new Text();

    public Text evaluate(Text nation) {
        String nationE = nation.toString();
        String nationCn = nationMap.get(nationE);
        if(nationCn == null) {
            text.set("alien");
            return text;
        }
        text.set(nationCn);
        return text;
    }
}
