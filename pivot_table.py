import itertools


def pre_example(sql_in, params):

    p_val = params.get("统计值", None)
    p_index = params.get("行维度", None)
    p_col = params.get("列维度", None)

    try:
        # 生成统计字段===============
        s_calc = ""
        if p_val:
            p_aggfunc = {}
            for p in p_val:
                i, f = list(map(str.strip, p.split(",")))
                if i in p_aggfunc:
                    if isinstance(p_aggfunc[i], list):
                        p_aggfunc[i].append(f)
                    else:
                        p_aggfunc[i] = [p_aggfunc[i], f]
                else:
                    p_aggfunc[i] = f

            # 拼接统计字段
            calc_keys = ""
            for k, v in p_aggfunc.items():
                if isinstance(v, list):
                    for fun in v:
                        calc_keys = calc_keys + fun + "(" + k + ")" + ", "
                else:
                    calc_keys = calc_keys + v + "(" + k + ")" + ", "
            s_calc = ", " + calc_keys.strip(", ")

        # 生成选择字段和分组字段========
        s_select = ""
        s_group = ""
        # 拼接分组字段
        if p_index or p_col:
            group_keys = ", ".join(
                (p_col if p_col else []) + (p_index if p_index else [])
            )
            s_select = "select " + group_keys
            s_group = " group by " + group_keys

        # 拼接From字段================
        s_from = " from (" + sql_in + ") as big_table"

        # 拼接整个SQL================
        if s_select:
            sql_run = s_select + s_calc + s_from + s_group
        else:
            sql_run = "select '请至少选择一个维度' as Tips"
    except Exception as e:
        sql_run = "select '" + str(e) + "'"
        # raise

    return sql_run


def get_dimensions(lst_cols, flt_cols=[]):
    """
    获取可用于透视表行和列的维度
    lst_cols    全部表头
    flt_cols    lst_cols子集，筛选需要展示的表头
    """
    if flt_cols:
        rst = list(
            filter(None, [x if x in flt_cols else "" for x in lst_cols])
        )
    else:
        rst = lst_cols

    return rst


def get_measures(
    lst_cols, flt_cols=[], func=["sum", "count", "max", "min", "avg"]
):
    """
    获取可用于透视表值的指标
    lst_cols    全部表头
    flt_cols    lst_cols子集，筛选需要展示的表头
    func        对字段的汇总方法，会与字段形成笛卡尔积，可自定义
    """
    measures = get_dimensions(lst_cols, flt_cols)
    rst = []
    for i in itertools.product(measures, func):
        rst.append(", ".join(i))
    return rst
