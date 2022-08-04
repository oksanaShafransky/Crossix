package sort;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Comparator;

@Data
@Getter
@Setter
@NoArgsConstructor
public class InputData implements Comparator<InputData> {
    String visitorId;
    String siteUrl;
    String pageViewUrl;
    long timestamp;

    @Override
    public String toString(){
        StringBuilder dataBuilder = new StringBuilder();
        appendFieldValue(dataBuilder, visitorId);
        appendFieldValue(dataBuilder, siteUrl);
        appendFieldValue(dataBuilder, pageViewUrl);
        appendFieldValue(dataBuilder, String.valueOf(timestamp));

        return dataBuilder.toString();
    }

    private void appendFieldValue(StringBuilder dataBuilder, String fieldValue) {
        if(fieldValue != null) {
            dataBuilder.append(fieldValue).append(",");
        } else {
            dataBuilder.append("").append(",");
        }
    }

    @Override
    public int compare(InputData o1, InputData o2) {
        return o1.getVisitorId().compareTo(o2.getVisitorId());
    }
}
