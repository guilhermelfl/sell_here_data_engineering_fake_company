from faker import Faker
from datetime import date, timedelta
import random
import pandas as pd
from math import floor
import uuid
from sqlalchemy import create_engine
import os
import shutil

def create_products_data(fake, creation_date):
    category_list = ['Home & Kitchen', 
                    'Beauty & Personal Care', 
                    'Toys & Games',
                    'Clothing',
                    'Health',
                    'Sports',
                    'Arts & Crafts',
                    'Books',
                    'Kitchen & Dining',
                    'Electronics',
                    'Pet Supplies',
                    'Office Products',
                    'Cell Phone & Accessories',
                    'Luggage & Travel Gear',
                    'Musical Instruments']
    prod_row = {}
    prod_row['product_id'] = uuid.uuid4()
    prod_row['product_name'] = fake.unique.sentence(random.randint(3,5))
    prod_row['base_product_price'] = random.uniform(1,200)
    prod_row['product_category'] = random.choice(category_list)
    prod_row['creation_date'] = creation_date
    return prod_row

def create_reseller_data(fake, creation_date):
    #cuit_number
    res_row = {}
    res_row['reseller_cnpj'] = fake.unique.cnpj()
    res_row['reseller_name'] = fake.bs()
    res_row['agreed_rate_pct'] = random.uniform(0.05,0.10)
    res_row['creation_date'] = creation_date

    return res_row


def create_customers_data(fake, creation_date):
    #document_number
    customer_row = {}
    customer_row['customer_cpf'] = fake.unique.cpf()
    customer_row['customer_name'] = fake.name()
    customer_row['customer_address'] = fake.address().replace('\n','|')
    customer_row['customer_email'] = fake.email()
    customer_row['birth_date'] = fake.date_of_birth(minimum_age = 21, maximum_age = 65)
    customer_row['creation_date'] = creation_date



    return customer_row


def create_reseller_products_data(fake, random_prod, random_res, creation_date):
    sup_prod_row = {}
    sup_prod_row ['reseller_cnpj'] = random_res['reseller_cnpj']
    sup_prod_row['product_id'] = random_prod['product_id']
    sup_prod_row['reseller_price'] = random_prod['base_product_price'] * random.uniform(1.12,1.3)
    sup_prod_row['creation_date'] = creation_date
    return sup_prod_row


def create_sales_data(fake, cust_cpf, res_prod_dict, creation_date, external_sales = False):
    payment_type_list = ['Debit','Credit','Pix']
    if external_sales:
        payment_type_list.append('Cash')
    sales_row = {}
    sales_row['sales_id'] = uuid.uuid4()
    sales_row['sales_datetime'] = creation_date + timedelta(hours = random.randint(0,23), minutes=random.randint(0,59), seconds = random.randint(0,59))
    sales_row['reseller_cnpj'] = res_prod_dict['reseller_cnpj']
    sales_row['product_id'] = res_prod_dict['product_id']
    sales_row['customer_cpf'] = cust_cpf
    sales_row['transaction_amount'] = res_prod_dict['reseller_price'] * random.uniform(0.8,1.3)
    sales_row['payment_type'] = random.choice(payment_type_list)
    sales_row['creation_date'] = creation_date
    return sales_row

def generate_data():
    fake = Faker('pt_BR')
    fake.random.seed(42)
    today = date.today()
    iter_date = today - timedelta(days=730)
    dir_name = 'reseller_files'
    shutil.rmtree(f'{dir_name}',ignore_errors=True)
    os.makedirs(f'{dir_name}', exist_ok=False)

    list_cust_dict = []
    list_reseller_dict = []
    list_products_dict = []
    list_reseller_products_dict = []
    list_sales_dict = []

    while iter_date < today:
        iter_date = iter_date + timedelta(days=1)
        print(f'generating data for date: {iter_date}')

        #creating customers data
        for i in range(random.randint(1,120)):
            list_cust_dict.append(create_customers_data(fake,iter_date))

        #creating reseller data
        for i in range(random.randint(1,5)):
            list_reseller_dict.append(create_reseller_data(fake,iter_date))

        #creating products data
        for i in range(random.randint(1,13)):
            list_products_dict.append(create_products_data(fake,iter_date))

        #creating supplier_products data
        for i in range(random.randint(5,20)):
            random_prod = list_products_dict[random.randint(0,len(list_products_dict)-1)]
            random_res = list_reseller_dict[random.randint(0,len(list_reseller_dict)-1)]
            list_reseller_products_dict.append(create_reseller_products_data(fake,random_prod,random_res,iter_date))
        
        #creating sales data
        #to scale sales with customers It'll be used with customer %
        
        for i in range(random.randint(2,25+ floor( len(list_cust_dict)  *random.uniform(0.01,0.03) ) ) ):
            customer_random_cpf = list_cust_dict[random.randint(0,len(list_cust_dict)-1)]
            random_res_prod = list_reseller_products_dict[random.randint(0,len(list_reseller_products_dict)-1)]
            list_sales_dict.append(create_sales_data(fake,customer_random_cpf['customer_cpf'],random_res_prod,iter_date))

        #generating excel external files
        for i in range(random.randint(1,5)):
            #choosing a random reseller to be the file owner and get the products it sells
            random_reseller_dict = list_reseller_dict[random.randint(0,len(list_reseller_dict)-1)]
            random_reseller = random_reseller_dict['reseller_cnpj']
            choose_df = pd.DataFrame(list_reseller_products_dict)
            choose_df = choose_df[choose_df['reseller_cnpj'] == f"{random_reseller}"]
            list_resel_sales_dict = []
            if not choose_df.empty:
                for j in range(random.randint(1,25)):
                    customer_random_cpf = list_cust_dict[random.randint(0,len(list_cust_dict)-1)]
                    list_resel_sales_dict.append(create_sales_data(fake,customer_random_cpf['customer_cpf'],choose_df.sample(1).to_dict(orient='records')[0],iter_date, True))
                new_reseller_file_df = pd.DataFrame(list_resel_sales_dict)
                clean_cnpj = random_reseller.replace('/','').replace('-','').replace('.','')
                new_reseller_file_df.to_excel(f'{dir_name}/{iter_date}_{clean_cnpj}.xlsx',header=True, index=False)


    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

    cust_df = pd.DataFrame(list_cust_dict)
    cust_df.to_sql("customers", engine, if_exists = 'append', schema='sell_here', index=False, chunksize = 500000)

    print(f'customers: {cust_df.shape}')

    res_df = pd.DataFrame(list_reseller_dict)
    res_df.to_sql("reseller", engine, if_exists = 'append', schema='sell_here', index=False, chunksize = 500000)

    print(f'reseller: {res_df.shape}')

    prod_df = pd.DataFrame(list_products_dict)
    prod_df.to_sql("products", engine, if_exists = 'append', schema='sell_here', index=False, chunksize = 500000)

    print(f'products: {prod_df.shape}')

    res_prod_df = pd.DataFrame(list_reseller_products_dict)
    res_prod_df.drop_duplicates(subset=['product_id', 'reseller_cnpj'], keep='first', inplace=True)
    res_prod_df.to_sql("reseller_products", engine, if_exists = 'append', schema='sell_here', index=False, chunksize = 500000)

    print(f'reseller_products: {res_prod_df.shape}')

    sales_df = pd.DataFrame(list_sales_dict)
    sales_df.to_sql("sales", engine, if_exists = 'append', schema='sell_here', index=False, chunksize = 500000)

    print(f'sales: {sales_df.shape}')

        
generate_data()


