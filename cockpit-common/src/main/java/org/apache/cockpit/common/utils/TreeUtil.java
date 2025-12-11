package org.apache.cockpit.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 树形结构处理工具
 */
@Slf4j
public class TreeUtil<T> {


    /**
     * 给Tree增加子节点
     */
    @SuppressWarnings("unchecked")
    private void addChildren2Tree(T head, T node, String child) {
        try {
            List<T> children = null;
            Class<?> clazz = Class.forName(head.getClass().getName());
            PropertyDescriptor fidPd = new PropertyDescriptor(child, clazz);
            // 获得GET方法
            Method getChildren = fidPd.getReadMethod();
            Object o = getChildren.invoke(head);
            if (o == null) {
                children = new ArrayList<T>();
            } else {
                children = (List<T>) o;
            }
            children.add(node);
            Method setChildren = fidPd.getWriteMethod();
            setChildren.invoke(head, children);
        } catch (Exception e) {
            log.error("生成树形结构时出粗！", e);
        }

    }

    /**
     * 生成树形的实例对象
     */
    public List<T> getTreeByList(List<T> list) {
        return getTreeByList(list, null, "key", "parentId", "children", false);
    }

    /**
     * 生成树形的实例对象
     *
     * @param root          根节点的父属性值
     * @param key           表示ID的属性名
     * @param fid           表示父ID的属性名
     * @param child         表示子节点的属性名 (注 此处会进行类型校验 必须为List<T>)
     * @param isIgnoreModel 是否舍弃出错的数据
     * @param list          list
     * @return treeVo
     */
    public List<T> getTreeByList(List<T> list, String root, String key, String fid, String child, boolean isIgnoreModel) {
        Map<String, Integer> indexMap = new HashMap<>(50);
        for (int i = 0; i < list.size(); i++) {
            indexMap.put((String) get(list.get(i), key), i);
        }
        List<T> rs = new ArrayList<>();
        for (T t : list) {
            Object fVal = get(t, fid);
            String pid = fVal == null ? null : (String) fVal;
            if (get(t, child) == null) {
                set(t, child, new ArrayList<T>());
            }

            if (root == null ? "".equals(pid) : root.equals(pid)) {
                rs.add(t);
            } else {
                if (indexMap.get(pid) == null) {
                    if (!isIgnoreModel) {
                        rs.add(t);
                    }
                } else {
                    addChildren2Tree(list.get(indexMap.get(pid)), t, child);
                }
            }
        }
        return rs;
    }


    /**
     * 反射获取字段值
     */
    private static Object get(Object t, String prop) {
        try {
            PropertyDescriptor propPd = new PropertyDescriptor(prop, Class.forName(t.getClass().getName()));
            Method propGet = propPd.getReadMethod();
            return propGet.invoke(t);
        } catch (Exception e) {
            log.error("反射获取字段信息时出错！", e);
        }
        return null;
    }

    /**
     * 反射设置字段值
     */
    private void set(Object t, String prop, Object val) {
        try {
            PropertyDescriptor propPd = new PropertyDescriptor(prop, Class.forName(t.getClass().getName()));
            Method propGet = propPd.getWriteMethod();
            propGet.invoke(t, val);
        } catch (Exception e) {
            log.error("反射设置字段值时出错！", e);
        }
    }
}
