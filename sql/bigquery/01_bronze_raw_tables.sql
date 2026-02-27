CREATE TABLE `project-4d474cb9-a1a5-457f-80b.retail_bronze.order_items`
(
  order_date TIMESTAMP,
  source STRING,
  ingestion_ts TIMESTAMP,
  final_price FLOAT64,
  quantity INT64,
  product_id INT64,
  order_id STRING,
  discount_pct INT64,
  unit_price FLOAT64,
  order_item_id STRING
);

CREATE TABLE `project-4d474cb9-a1a5-457f-80b.retail_bronze.users`
(
  source STRING,
  role STRING,
  crypto STRUCT<network STRING, wallet FLOAT64, coin STRING>,
  userAgent STRING,
  ssn STRING,
  ein STRING,
  company STRUCT<title STRING, name STRING, address STRUCT<postalCode INT64, coordinates STRUCT<lng FLOAT64, lat FLOAT64>, state STRING, city STRING, stateCode STRING, country STRING, address STRING>, department STRING>,
  address STRUCT<postalCode INT64, coordinates STRUCT<lng FLOAT64, lat FLOAT64>, state STRING, city STRING, stateCode STRING, country STRING, address STRING>,
  bank STRUCT<iban STRING, currency STRING, cardType STRING, cardNumber INT64, cardExpire STRING>,
  university STRING,
  ip STRING,
  username STRING,
  macAddress STRING,
  age INT64,
  eyeColor STRING,
  bloodGroup STRING,
  image STRING,
  hair STRUCT<type STRING, color STRING>,
  weight FLOAT64,
  password STRING,
  email STRING,
  ingestion_ts TIMESTAMP,
  height FLOAT64,
  id INT64,
  gender STRING,
  firstName STRING,
  lastName STRING,
  phone STRING,
  birthDate DATE,
  maidenName STRING
);

CREATE TABLE `project-4d474cb9-a1a5-457f-80b.retail_bronze.products`
(
  thumbnail STRING,
  source STRING,
  meta STRUCT<qrCode STRING, barcode INT64, updatedAt TIMESTAMP, createdAt TIMESTAMP>,
  returnPolicy STRING,
  dimensions STRUCT<height FLOAT64, depth FLOAT64, width FLOAT64>,
  availabilityStatus STRING,
  discountPercentage FLOAT64,
  shippingInformation STRING,
  brand STRING,
  reviews ARRAY<STRUCT<reviewerEmail STRING, date TIMESTAMP, comment STRING, reviewerName STRING, rating INT64>>,
  title STRING,
  weight INT64,
  warrantyInformation STRING,
  stock INT64,
  images ARRAY<STRING>,
  price FLOAT64,
  tags ARRAY<STRING>,
  ingestion_ts TIMESTAMP,
  minimumOrderQuantity INT64,
  category STRING,
  description STRING,
  sku STRING,
  rating FLOAT64,
  id INT64
);

CREATE TABLE `project-4d474cb9-a1a5-457f-80b.retail_bronze.orders`
(
  payment_method STRING,
  currency STRING,
  order_date TIMESTAMP,
  source STRING,
  ingestion_ts TIMESTAMP,
  user_id INT64,
  total_amount_local FLOAT64,
  order_status STRING,
  discount_amount FLOAT64,
  total_amount_usd FLOAT64,
  order_id STRING
);




