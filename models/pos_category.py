import pandas as pd
from utils.postgres import select_df


def download_excel():
    sql = """
        with it as (
            select *
            from ir_translation
            where name = 'pos.category,name'
        )
        select
            concat('occ_kdosh.pos_category_', pc.id) as id,
            it.value as name
        from pos_category pc
        left join it
            on pc.id = it.res_id;
    """

    df_result = select_df(sql)
    file_name = "pos_category.xlsx"
    writer = pd.ExcelWriter(f"import/{file_name}", engine="xlsxwriter")
    df_result.to_excel(writer, sheet_name=file_name, index=False)
    writer.close()
