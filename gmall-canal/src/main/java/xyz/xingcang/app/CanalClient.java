package xyz.xingcang.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import xyz.xingcang.gmall_constants.TopicConstants;
import xyz.xingcang.util.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author xingcang
 * @create 2020-11-06 7:04 PM
 */
public class CanalClient {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop001", 11111),
                "example",
                "",
                ""
        );

        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall.*");
            Message message = canalConnector.get(100);
            if (message.getEntries().size() <= 0) {
                System.out.println("没有数据，等一会儿");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            String tableName = entry.getHeader().getTableName();
                            ByteString storeValue = entry.getStoreValue();
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            handler(tableName, eventType, rowDataList);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT == eventType) {
            for (CanalEntry.RowData rowData : rowDataList) {
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                MyKafkaSender.Send(TopicConstants.GMALL_ORDER_INFO, jsonObject.toJSONString());
            }
        }
    }
}
