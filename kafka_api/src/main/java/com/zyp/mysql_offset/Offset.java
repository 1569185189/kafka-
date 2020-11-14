package com.zyp.mysql_offset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Date;

/**
 * create by
 *
 * @author zouyuanpeng
 * @date 2020/11/7 17:54
 */
//自定义存储offset
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Offset {
    //主键
    private Integer id;
    //主题
    private String subject;
    //分区
    private Integer partition;
    //偏移量offset
    private Long offset;
    //保存时间
    private String saveDate;

    public Offset(String subject, Integer partition, Long offset, String saveDate) {
        this.subject = subject;
        this.partition = partition;
        this.offset = offset;
        this.saveDate = saveDate;
    }
}
