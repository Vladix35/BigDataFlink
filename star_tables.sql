DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER UNIQUE,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INTEGER,
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50),
    pet_type VARCHAR(50),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER UNIQUE,
    product_name VARCHAR(50),
    category VARCHAR(50),
    price REAL,
    weight REAL,
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(50),
    material VARCHAR(50),
    description VARCHAR(1024),
    rating REAL,
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    pet_category VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dim_seller;
CREATE TABLE dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id INTEGER UNIQUE,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dim_store;
CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id INTEGER UNIQUE,
    store_name VARCHAR(50),
    location VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    phone VARCHAR(50),
    email VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dim_supplier;
CREATE TABLE dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_id INTEGER UNIQUE,
    supplier_name VARCHAR(50),
    contact VARCHAR(50),
    email VARCHAR(50),
    phone VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS fact_sale;
CREATE TABLE fact_sale (
    sale_key SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    seller_key INTEGER REFERENCES dim_seller(seller_key),
    store_key INTEGER REFERENCES dim_store(store_key),
    supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    total_price REAL NOT NULL,
    discount REAL DEFAULT 0,
    shipping_cost REAL DEFAULT 0,
    tax_amount REAL DEFAULT 0,
    net_price REAL NOT NULL,
    original_sale_id VARCHAR(100),
    payment_method VARCHAR(50),
    delivery_status VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);