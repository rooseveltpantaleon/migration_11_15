import pandas as pd
from utils.postgres import select_df


def download_excel():
    sql = """
        with it as (
        select *
        from ir_translation
        where name = 'product.attribute,name'
        )
        select
            concat('occ_kdosh.product_attribute_', pa.id) as Id,
            it.value as Attribute,
            -- 'Radio' as "Display Type",
            'Instantly' as "Variants Creation Mode"
        from product_attribute pa
        left join it
            on pa.id = it.res_id;
    """

    df_result = select_df(sql)
    file_name = "product_attribute.xlsx"
    writer = pd.ExcelWriter(f"import/{file_name}", engine="xlsxwriter")
    df_result.to_excel(writer, sheet_name=file_name, index=False)
    writer.close()
