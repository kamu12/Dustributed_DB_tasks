-- SOURCE '/home/yura/Dropbox/UCU/Distributed_DB/tasks/cassandra.sql'

DROP KEYSPACE IF EXISTS store;
-- DESCRIBE keyspaces;

CREATE KEYSPACE store
WITH REPLICATION = {
		'class':'SimpleStrategy',
		'replication_factor':1
	};

use store;

CREATE TABLE items(
	category text,
	name text,
	price double,
	producer text,
	properties map<text, text>,
	PRIMARY KEY((category), price, name));

CREATE INDEX name_ind ON items(name);
CREATE INDEX producer_ind ON items(producer);

-- fill data
INSERT INTO items JSON '{
	"category" : "Phone",
	"name" : "iPhone 6",
	"producer" : "Apple",
	"price" : 600,
	"properties":
		{"battery_capacity":"2700"}}';

INSERT INTO items JSON '{
	"category" : "Phone",
	"name" : "S7 EDGE",
	"producer" : "Samsung",
	"price" : 420,
	"properties":
		{"battery_capacity":"3600"}}';

INSERT INTO items JSON '{
	"category" : "TV",
	"name" : "UQHD 40",
	"producer" : "Samsung",
	"price" : 1600,
	"properties":
		{"diagonal":"40"}}';

INSERT INTO items JSON '{
	"category" : "Fitness tracker",
	"name" : "Mi Band 2",
	"producer" : "Xiomi",
	"price" : 20,
	"properties":
		{"color":"black"}}';

INSERT INTO items JSON '{
	"category" : "TV",
	"name" : "Rift",
	"producer" : "Oculus",
	"price" : 400,
	"properties":
		{"accessoires":"trackpad"}}';

INSERT INTO items JSON '{
	"category" : "Phone",
	"name" : "Pixel 2",
	"producer" : "Google",
	"price" : 800,
	"properties":
		{"battery_capacity":"2800"}}';

-- Part 1
-- Task1
DESCRIBE store.items;	
-- Task2
SELECT * FROM items WHERE category='Phone' ORDER BY price DESC;
-- Task3
SELECT * FROM items WHERE category='TV' AND name='Rift';
SELECT * FROM items WHERE category='Phone' AND price > 450 AND price < 800;
SELECT * FROM items WHERE category='Phone' AND price > 450 AND producer = 'Google';
-- Task4
SELECT * FROM items WHERE category='Phone'
	AND properties contains key 'battery_capacity' ALLOW FILTERING;

SELECT * FROM items WHERE category='Phone'
	AND properties contains key 'battery_capacity'
	AND properties['battery_capacity'] = '3600' ALLOW FILTERING;
-- Task5
SELECT * FROM items WHERE category='Phone' AND price = 420 AND name='S7 EDGE';

UPDATE items set properties['battery_capacity'] = '3500'
WHERE category='Phone' AND price = 420 AND name='S7 EDGE';

UPDATE items set properties = properties + {'color':'black'}
WHERE category='Phone' AND price = 420 AND name='S7 EDGE';

SELECT * FROM items WHERE category='Phone' AND price = 420 AND name='S7 EDGE';

UPDATE items set properties = properties - {'battery_capacity'}
WHERE category='Phone' AND price = 420 AND name='S7 EDGE';

SELECT * FROM items WHERE category='Phone' AND price = 420 AND name='S7 EDGE';

-- Part 2

CREATE TABLE orders(
	customer_name text,
	order_time timestamp,
	cost double, 
	items set<text>,
	PRIMARY KEY((customer_name), order_time));

CREATE INDEX items_ind ON orders(items);

INSERT INTO orders(customer_name, order_time, cost, items)
	VALUES('Yura Kaminskyi', '2018-02-15', 850, {'phone', 'headphones'});
INSERT INTO orders(customer_name, order_time, cost, items)
	VALUES('Yura Priyma', '2018-02-16', 1200, {'wallet', 'mouse'});
INSERT INTO orders(customer_name, order_time, cost, items)
	VALUES('Yura Mykhalchuk', '2018-02-17', 51000, {'notebook', 'iBottle'});
INSERT INTO orders(customer_name, order_time, cost, items)
	VALUES('Yura Kaminskyi', '2018-02-18', 50, {'products'});
INSERT INTO orders(customer_name, order_time, cost, items)
	VALUES('Yura Kaminskyi', '2018-02-22', 250, {'chair'});
-- Task1
DESCRIBE orders;
-- Task2
SELECT * FROM orders WHERE customer_name='Yura Kaminskyi' ORDER BY order_time DESC;
-- Task3
SELECT * FROM orders WHERE customer_name='Yura Kaminskyi' AND items contains 'products';
-- Task4
SELECT customer_name, COUNT(cost) FROM orders WHERE customer_name='Yura Kaminskyi' AND order_time < '2018-02-20';
-- Task5
SELECT customer_name, AVG(cost) FROM orders GROUP BY customer_name;
-- Task6
SELECT customer_name, SUM(cost) FROM orders GROUP BY customer_name;
-- Task7
SELECT customer_name, MAX(cost) FROM orders GROUP BY customer_name;
-- Task8
SELECT * FROM orders WHERE customer_name='Yura Kaminskyi' and order_time = '2018-02-15';

UPDATE orders SET items = items + {'keyboard'}, cost = 950
WHERE customer_name='Yura Kaminskyi' and order_time = '2018-02-15';

SELECT * FROM orders WHERE customer_name='Yura Kaminskyi' and order_time = '2018-02-15';
-- Task9
SELECT customer_name, items, writetime(cost) FROM orders GROUP BY customer_name, order_time;
-- Task10
INSERT INTO orders(customer_name, order_time, cost, items)
	VALUES('Yura Mykhalchuk', '2018-02-23', 250, {'chair'}) using TTL 1;

SELECT * FROM orders WHERE customer_name='Yura Mykhalchuk';

CREATE KEYSPACE temp
WITH REPLICATION = {
		'class':'SimpleStrategy',
		'replication_factor':1
	};
DROP KEYSPACE temp;


CREATE KEYSPACE temp
WITH REPLICATION = {
		'class':'SimpleStrategy',
		'replication_factor':1
	};
DROP KEYSPACE temp;

CREATE KEYSPACE temp
WITH REPLICATION = {
		'class':'SimpleStrategy',
		'replication_factor':1
	};
DROP KEYSPACE temp;

CREATE KEYSPACE temp
WITH REPLICATION = {
		'class':'SimpleStrategy',
		'replication_factor':1
	};
DROP KEYSPACE temp;

CREATE KEYSPACE temp
WITH REPLICATION = {
		'class':'SimpleStrategy',
		'replication_factor':1
	};
DROP KEYSPACE temp;

CREATE KEYSPACE temp
WITH REPLICATION = {
		'class':'SimpleStrategy',
		'replication_factor':1
	};
DROP KEYSPACE temp;

SELECT * FROM orders WHERE customer_name='Yura Mykhalchuk';
-- Task11
SELECT JSON * FROM orders WHERE customer_name='Yura Kaminskyi' ORDER BY order_time DESC;
-- Task12
INSERT INTO orders JSON '{
		"customer_name": "Yura Mykhalchuk",
		"order_time": "2018-02-27", 
		"cost": 456,
		"items":["wiskey", "ice"]
	}';

SELECT * FROM orders WHERE customer_name='Yura Mykhalchuk';
