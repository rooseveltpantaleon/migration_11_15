import pandas as pd
from utils.postgres import select_df


def download_excel():
    sql = """
        with it as (
            select *
            from ir_translation
            where name = 'product.attribute.value,name'
        )
        select
            concat('occ_kdosh.product_attribute_value_', pav.id) as Id,
            concat('occ_kdosh.product_attribute_', pa.id) as "attribute_id/id",
            it.value as name
        from product_attribute_value pav
        left join product_attribute pa
            on pav.attribute_id = pa.id
        left join it
            on pav.id = it.res_id;
    """

    df_result = select_df(sql)
    file_name = "product_attribute_value.xlsx"
    writer = pd.ExcelWriter(f"import/{file_name}", engine="xlsxwriter")
    df_result.to_excel(writer, sheet_name=file_name, index=False)
    writer.close()
