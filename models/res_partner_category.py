import pandas as pd
from utils.postgres import select_df


def download_excel():
    sql = """
        with it as (
            select *
            from ir_translation
            where name = 'res.partner.category,name'
        )
        select
            concat('occ_kdosh.res_partner_category_', rpc.id) as id,
            it.value as name
        from res_partner_category rpc
        left join it
            on rpc.id = it.res_id;
    """

    df_result = select_df(sql)
    file_name = "res_partner_category.xlsx"
    writer = pd.ExcelWriter(f"import/{file_name}", engine="xlsxwriter")
    df_result.to_excel(writer, sheet_name=file_name, index=False)
    writer.close()
