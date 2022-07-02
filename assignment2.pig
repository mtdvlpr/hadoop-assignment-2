-- Load scv data file using pig storage
orders = LOAD '/user/maria_dev/diplomacy/orders.csv' USING PigStorage(',') AS
(game_id:chararray,
unit_id:chararray,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:chararray,
reason:chararray,
turn_num:chararray);

-- Filter orders where target equals "Holland"
filtered_orders = FILTER orders BY target == '"Holland"';

-- Group orders by location, target
grouped_orders = GROUP filtered_orders BY (location, target);

-- Count how many times each location targeted Holland
locations_counted = FOREACH grouped_orders GENERATE group, COUNT(filtered_orders);

-- Sort orders alphabetically
sorted_orders = ORDER locations_counted BY $0 ASC;

-- Dump result
DUMP sorted_orders;