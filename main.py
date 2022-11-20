from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.


from models import product_attribute


if __name__ == '__main__':
    product_attribute.download_excel()
