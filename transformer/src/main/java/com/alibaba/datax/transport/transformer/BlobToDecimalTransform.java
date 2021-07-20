package com.alibaba.datax.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class BlobToDecimalTransform extends com.alibaba.datax.transformer.Transformer {

    int columnIndex;

    public BlobToDecimalTransform(){
        setTransformerName("dx_trans_blob2deciamal");
        System.out.println("Using BlobToDecimalTransform preserve masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("dx_trans_blob2deciamal transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            byte[] bytesRead =  column.asBytes();//.getRawData();
            if (bytesRead == null) {
                return record;
            }
//            byte[] bytesRead = IOUtils.toByteArray(oriValue.getBinaryStream());
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytesRead).order(ByteOrder.BIG_ENDIAN);
            //-java默认是big-endian
            String newValue = String.valueOf(byteBuffer.getLong());
            record.setColumn(columnIndex, new StringColumn(newValue));
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
