from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.


from models import (
    loyalty_program,
    pos_category,
    pos_config,
    product_attribute_value,
    product_attribute,
    product_category,
    product_product_link,
    product_template,
    res_partner_category,
    res_partner,
    stock_picking,
    stock_warehouse,
    load_invoices_11,
)


if __name__ == "__main__":
    product_attribute.download_excel()
    product_attribute_value.download_excel()
    product_category.download_excel()
    pos_category.download_excel()
    res_partner_category.download_excel()
    res_partner.download_excel()

    stock_warehouse.create()
