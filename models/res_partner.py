import pandas as pd
from utils.postgres import select_df


def download_excel():
    sql = """
        with contact as (
            select
                concat('occ_kdosh.res.partner_', rp.id) as id,
                rp.name as name,
                rp.street as street,
                rp.doc_number as vat,
                rp.country_id as "country_id/.id",
                rp.province_id as "city_id/.id",
                -- 1163 as state_id,
                rp.zip as zip,
                rp.phone as phone,
                rp.mobile as mobile,
                rp.is_company as is_company,
                rp.email as email,
                rp.website as website,
                rp.loyalty_points as loyalty_points,
                case when rp.supplier = true then 1 else 0 end as supplier_rank,
                case when rp.supplier = false then 1 else 0 end as customer_rank,
                case when rp.is_company
                    then 4
                    else 5
                end as "l10n_latam_identification_type_id/.id",
                'es_PE' as lang,
                case when rprpcr.category_id is null
                    then ''
                    else concat('occ_kdosh.res_partner_category_', rprpcr.category_id)
                end as "category_id/id"
            from res_partner rp
            left join res_partner_res_partner_category_rel rprpcr
                on rp.id = rprpcr.partner_id
            where rp.active
        )
        select id,
            name,
            street,
            vat,
            "country_id/.id",
            "city_id/.id",
            zip,
            phone,
            mobile,
            is_company,
            email,
            website,
            loyalty_points,
            supplier_rank,
            customer_rank,
            "l10n_latam_identification_type_id/.id",
            lang,
            string_agg("category_id/id", ',') as "category_id/id"
        from contact
        group by id, name, street, vat, "country_id/.id", "city_id/.id", zip,
                phone, mobile, is_company, email, website, loyalty_points,
                supplier_rank, customer_rank, "l10n_latam_identification_type_id/.id", lang;
    """

    df_result = select_df(sql)
    # df_result["vat"] = df_result["vat"].str.replace(r"\D", "")
    file_name = "res_partner.xlsx"
    writer = pd.ExcelWriter(f"import/{file_name}", engine="xlsxwriter")
    df_result.to_excel(writer, sheet_name=file_name, index=False)
    writer.close()
