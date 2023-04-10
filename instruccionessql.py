DDL_QUERY =  '''
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE,
    status_id INTEGER,
    status VARCHAR(255),
    item_id INTEGER,
    sku VARCHAR(255),
    qty_ordered INTEGER,
    price NUMERIC,
    value NUMERIC,
    discount_amount NUMERIC,
    total NUMERIC,
    payment_method_id INTEGER,
    payment_method VARCHAR(255),
    cust_id INTEGER,
    ref_num INTEGER,
    name_prefix VARCHAR(10),
    first_name VARCHAR(255),
    middle_initial CHAR(1),
    last_name VARCHAR(255),
    gender CHAR(1),
    age INTEGER,
    full_name VARCHAR(255),
    email VARCHAR(255),
    customer_since DATE,
    ssn VARCHAR(11),
    phone_no VARCHAR(15),
    direccion_id INTEGER,
    place_name VARCHAR(255),
    county VARCHAR(255),
    city VARCHAR(255),
    state CHAR(2),
    zip VARCHAR(10),
    region VARCHAR(255),
    user_name VARCHAR(255),
    discount_percent NUMERIC
);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    sku VARCHAR(255),
    category_id INTEGER
);

CREATE TABLE IF NOT EXISTS addresses (
    direccion_id INTEGER PRIMARY KEY,
    place_name VARCHAR(255),
    county VARCHAR(255),
    city VARCHAR(255),
    state CHAR(2),
    zip VARCHAR(10),
    region VARCHAR(255)
);

ALTER TABLE orders
ADD CONSTRAINT fk_orders_products
FOREIGN KEY (item_id) REFERENCES products (product_id) ON DELETE CASCADE;

ALTER TABLE orders
ADD CONSTRAINT fk_orders_addresses
FOREIGN KEY (direccion_id) REFERENCES addresses (direccion_id) ON DELETE CASCADE;



 '''