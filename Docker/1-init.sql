create schema IF NOT EXISTS sell_here;

create table IF NOT EXISTS sell_here.reseller
(
reseller_cnpj VARCHAR (255) primary key,
reseller_name VARCHAR ( 255 ) not null,
agreed_rate_pct float not null,
creation_date date not null
);

create table IF NOT EXISTS sell_here.products
(
product_id VARCHAR (255) primary key,
product_name VARCHAR (255) not null,
base_product_price float not null,
product_category varchar(255) not null,
creation_date date not null
);

create table IF NOT EXISTS sell_here.reseller_products
(
reseller_cnpj VARCHAR (255) not null,
product_id VARCHAR (255) not null,
reseller_price float,
creation_date date not null,
CONSTRAINT fk_rp_product_id FOREIGN KEY(product_id) REFERENCES sell_here.products(product_id),
CONSTRAINT fk_rp_reseller_cnpj FOREIGN KEY(reseller_cnpj) REFERENCES sell_here.reseller(reseller_cnpj),
UNIQUE (product_id, reseller_cnpj)
);


create table IF NOT EXISTS sell_here.customers
(
customer_cpf VARCHAR (255) primary key,
customer_name VARCHAR (255) not null,
customer_address VARCHAR (255) not null,
customer_email VARCHAR (255) not null,
birth_date date not null,
creation_date date not null
);


create table IF NOT EXISTS sell_here.sales
(
sales_id VARCHAR (255) primary key,
sales_datetime timestamp not null,
reseller_cnpj  VARCHAR (255) not null,
product_id  VARCHAR (255) not null,
customer_cpf  VARCHAR (255) not null,
transaction_amount float not null,
payment_type VARCHAR (255) not null,
creation_date date not null,
CONSTRAINT fk_sales_customer_cpf FOREIGN KEY(customer_cpf) REFERENCES sell_here.customers(customer_cpf),
CONSTRAINT fk_sales_reseller_cnpj FOREIGN KEY(reseller_cnpj) REFERENCES sell_here.reseller(reseller_cnpj),
CONSTRAINT fk_sales_product_id FOREIGN KEY(product_id) REFERENCES sell_here.products(product_id)
);


create schema IF NOT EXISTS dw;
create schema IF NOT EXISTS external_data;