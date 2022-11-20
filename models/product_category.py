import pandas as pd
from utils.postgres import select_df


def download_excel():
    for level in range(1,4):
        sql = f"""
            with recursive category_recursive (level, id, parent_id) as (
                select 1, pc.id, pc.parent_id
                from product_category pc
                where pc.parent_id is null
                union all
                select cr.level + 1, pc2.id, pc2.parent_id
                from product_category pc2
                    inner join category_recursive cr
                    on cr.id = pc2.parent_id
            ),
            it as (
                select *
                from ir_translation
                where name = 'product.category,name'
            )
            select
                concat('occ_kdosh.product_category_', pc.id) as id,
                case when cr.level = 1 then null
                    else concat('occ_kdosh.product_category_', pc.parent_id) end as "parent_id/id",
                it.value as name
            from product_category pc
            left join it
                on pc.id = it.res_id
            left join category_recursive cr
                on pc.id = cr.id
            where level = {level};
        """

        df_result = select_df(sql)
        file_name = f"product_category_{level}.xlsx"
        writer = pd.ExcelWriter(f"import/{file_name}", engine="xlsxwriter")
        df_result.to_excel(writer, sheet_name=file_name, index=False)
        writer.close()
