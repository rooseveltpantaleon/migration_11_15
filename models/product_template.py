import pandas as pd
from utils.postgres import select_df


def download_excel():
    sql = """
        with it as (
            select *
            from ir_translation
            where name = 'product.template,name'
        )
        , product_template_attr as (
            select
                concat('occ_kdosh.product_template_', pt.id) as id,
                it.value as name,
                pt.default_code as default_code,
                (pt.list_price + (pt.list_price * 0.18)) as list_price,
                0 as standard_price,
                'Unidades' as uom_id,
                'Storable Product' as type,
                'True' as sale_ok,
                'True' as purchase_ok,
                'True' as active,
                'True' as available_in_pos,
                concat('occ_kdosh.product_category_', pt.categ_id) as "categ_id/id",
                case when pt.pos_categ_id is null then '' else concat('occ_kdosh.pos_category_', pt.pos_categ_id) end as "pos_categ_id/id",
                case when pav.attribute_id is null then '' else concat('occ_kdosh.product_attribute_', pav.attribute_id) end as "attribute_line_ids/attribute_id/id",
                case when pavppr.product_attribute_value_id is null then '' else concat('occ_kdosh.product_attribute_value_', pavppr.product_attribute_value_id) end as "attribute_line_ids/value_ids/id",
                pavppr.product_attribute_value_id
            from product_template pt
            left join it
                on pt.id = it.res_id
            left join product_product pp
                on pt.id = pp.product_tmpl_id
            left join product_attribute_value_product_product_rel pavppr
                on pp.id = pavppr.product_product_id
            left join product_attribute_value pav
                on pavppr.product_attribute_value_id = pav.id
            left join product_attribute_price pap
                on pav.id = pap.value_id and pp.product_tmpl_id = pap.product_tmpl_id
            where pt.active = true
            -- and pt.id in (10, 12, 49989, 50126, 50127)
        )
        select id,
            name,
            default_code,
            list_price,
            standard_price,
            uom_id,
            type,
            sale_ok,
            purchase_ok,
            active,
            available_in_pos,
            "categ_id/id",
            "pos_categ_id/id",
            "attribute_line_ids/attribute_id/id"                        as "attribute_line_ids/attribute_id/id",
            string_agg(distinct "attribute_line_ids/value_ids/id", ',') as "attribute_line_ids/value_ids/id"
        from product_template_attr
        group by id, name, default_code, list_price, standard_price, uom_id, type, sale_ok, purchase_ok, active,
            available_in_pos, "categ_id/id", "pos_categ_id/id", "attribute_line_ids/attribute_id/id"
    """

    df_result = select_df(sql)
    file_name = "product_template.xlsx"
    writer = pd.ExcelWriter(f"import/{file_name}", engine="xlsxwriter")
    df_result.to_excel(writer, sheet_name=file_name, index=False)
    writer.close()
