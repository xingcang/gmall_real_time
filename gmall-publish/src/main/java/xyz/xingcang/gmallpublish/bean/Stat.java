package xyz.xingcang.gmallpublish.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author xingcang
 * @create 2020-11-13 09:11
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Stat {
    private String title;
    private List<Option> options;
}
